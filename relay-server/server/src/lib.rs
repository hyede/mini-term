//! 中转服务器核心:axum Router、桌面端/移动端 WebSocket 端点与配对状态机。
//!
//! 以 lib 形式暴露 `app()` / `RelayState`,让 Seam 1 测试进程内启动真实服务、
//! 用真实协议帧从边界驱动;`main.rs` 只负责读环境变量并绑定端口。
//!
//! 中转纪律:消息体仅内存转发不落盘;日志只记元数据(连接、鉴权结果),不记消息内容。
//! 设备和长期授权可持久化;一次性配对码与连接/订阅/待回执路由只存内存。
//!
//! # 资源上限(公网服务,未鉴权的连接也要先占资源)
//!
//! - **单条消息**:按端点分开设([`DESKTOP_MAX_MESSAGE_BYTES`] / [`MOBILE_MAX_MESSAGE_BYTES`]),
//!   取代 axum 默认的 64 MiB。限额在升级时定死,握手前的那条 hello 也受它约束。
//! - **连接数**:全局 + 按客户端 IP 两道闸([`RelayLimits`]),在 WebSocket 升级**之前**判,
//!   超限直接回 HTTP 503 / 429,不进握手。
//! - **出站队列**:每条连接有界([`RelayLimits::outbound_queue`]),满了 = 对端消费太慢,
//!   断开该连接而不是丢帧,理由见 [`ConnSlot::send`]。

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::connect_info::ConnectInfo;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::{HeaderMap, HeaderName, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use axum::{Extension, Router};
use mt_relay_protocol::DesktopRejectReason;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, Notify};

/// 握手超时:连上后必须在此时限内送达 hello,否则直接断开。
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// 一次性配对码有效期。
const PAIRING_CODE_TTL: Duration = Duration::from_secs(10 * 60);

/// 桌面端单条入站消息上限:16 MiB。
///
/// 桌面端发来的消息里只有对话镜像(`mirrorSnapshot` / `mirrorHistory` 一页 50 条文本、
/// `mirrorAppend`)能长大:协议里没有图片或二进制载荷,镜像只抽会话记录里的
/// user / assistant **文本**,外加工具调用的一行摘要(工具名 + 截到 160 字的目标,
/// 单条几百字节,不计入那 50 条、单页总数另有封顶;不含工具输出)。
/// 常见一页几十 KB 到几百 KB;极端情况是用户往 AI 里贴过大段日志,一页 50 条里带几条 MB 级正文,也就几 MB。16 MiB 给这种
/// 极端情况留出三倍以上余量,同时比默认的 64 MiB 收紧四倍 —— 升级前的这一跳谁都能连,
/// 默认值等于允许任何人让中转为每条连接先攒 64 MiB。
pub const DESKTOP_MAX_MESSAGE_BYTES: usize = 16 << 20;

/// 移动端单条入站消息上限:1 MiB。
///
/// 移动端上行只有握手、订阅 / 退订、翻页请求、点选作答、改名(桌面端截 64 字)、
/// 发起会话与移动端指令 —— 唯一的自由文本是指令正文,手机上敲 / 粘贴的内容到 KB 级
/// 已经很长。1 MiB 是千倍量级的余量。
pub const MOBILE_MAX_MESSAGE_BYTES: usize = 1 << 20;

/// 全局并发 WebSocket 连接数上限的默认值(`RELAY_MAX_CONNECTIONS`)。
///
/// 中转是单租户 1×1 拓扑:正常只有一条桌面端 + 一条移动端,顶替 / 重连的瞬间多出
/// 一两条。64 远高于正常用量,又把「每条连接先攒一条最大消息」的最坏内存压在
/// 64 × 16 MiB = 1 GiB 以内。
pub const DEFAULT_MAX_CONNECTIONS: usize = 64;

/// 单个客户端 IP 并发连接数上限的默认值(`RELAY_MAX_CONNECTIONS_PER_IP`)。
///
/// ⚠️ 反代后面部署时必须配 [`RelayLimits::client_ip_header`],否则所有连接的对端地址
/// 都是反代自己,这道闸就退化成第二道全局闸(16 条仍够正常使用,只是挡不住单一来源
/// 占满全局名额)。
pub const DEFAULT_MAX_CONNECTIONS_PER_IP: usize = 16;

/// 每条连接出站队列容量(帧数)的默认值。
///
/// 正常流量是零星的结构增量、1s 一轮的镜像增量与回执,手机切回前台时的突发
/// (ack + presence + 快照 + 若干镜像页)也就十来帧。积压到 256 帧 = 对端已经落后
/// 很多轮,断开重连比继续攒更快恢复。
pub const DEFAULT_OUTBOUND_QUEUE: usize = 256;

/// 连接相关的资源上限。默认值见各常量;`main.rs` 从环境变量覆盖。
#[derive(Debug, Clone)]
pub struct RelayLimits {
    /// 全局并发 WebSocket 连接数(含尚未握手的)
    pub max_connections: usize,
    /// 单个客户端 IP 的并发连接数
    pub max_connections_per_ip: usize,
    /// 按 IP 计数时从哪个请求头取客户端地址(`RELAY_CLIENT_IP_HEADER`,如
    /// `CF-Connecting-IP`)。`None` = 用 TCP 对端地址。头里有多个逗号分隔的值时取
    /// **最后一个**(离中转最近的那一跳写的);头缺失或解析不出 IP 时回落对端地址。
    /// 只在中转前面确有反代改写该头时才配 —— 否则客户端可以自报任意值绕开这道闸
    /// (全局闸不受影响)。
    pub client_ip_header: Option<HeaderName>,
    /// 每条连接的出站队列容量(帧)
    pub outbound_queue: usize,
}

impl Default for RelayLimits {
    fn default() -> Self {
        Self {
            max_connections: DEFAULT_MAX_CONNECTIONS,
            max_connections_per_ip: DEFAULT_MAX_CONNECTIONS_PER_IP,
            client_ip_header: None,
            outbound_queue: DEFAULT_OUTBOUND_QUEUE,
        }
    }
}

/// 当前在册的连接计数(全局 + 按 IP)。条目随 [`ConnPermit`] 析构递减,归零即删,
/// 所以 `per_ip` 的大小不会超过全局上限。
#[derive(Default)]
struct ConnCounts {
    total: usize,
    per_ip: HashMap<IpAddr, usize>,
}

/// 一条 WebSocket 连接占用的名额。升级前领取,随连接任务一起活到连接结束;
/// 升级没成(客户端半路走了)时随 `on_upgrade` 的闭包一起析构,名额同样归还。
struct ConnPermit {
    counts: Arc<Mutex<ConnCounts>>,
    ip: IpAddr,
}

impl Drop for ConnPermit {
    fn drop(&mut self) {
        // 析构里不能 panic(可能正处于展开中):锁中毒也照常归还名额
        let mut counts = self.counts.lock().unwrap_or_else(|e| e.into_inner());
        counts.total = counts.total.saturating_sub(1);
        if let Some(n) = counts.per_ip.get_mut(&self.ip) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                counts.per_ip.remove(&self.ip);
            }
        }
    }
}

/// 名额不够时的拒绝理由(映射成 HTTP 状态码)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdmitError {
    /// 全局满 → 503
    Global,
    /// 该 IP 满 → 429
    PerIp,
}

impl IntoResponse for AdmitError {
    fn into_response(self) -> Response {
        match self {
            AdmitError::Global => (
                StatusCode::SERVICE_UNAVAILABLE,
                "relay connection limit reached",
            )
                .into_response(),
            AdmitError::PerIp => (
                StatusCode::TOO_MANY_REQUESTS,
                "too many connections from this client",
            )
                .into_response(),
        }
    }
}

/// 解析计数用的客户端地址(口径见 [`RelayLimits::client_ip_header`])。
/// 两样都拿不到(进程内测试没挂 ConnectInfo)时归到 0.0.0.0 这一个桶。
fn client_ip(limits: &RelayLimits, headers: &HeaderMap, peer: Option<SocketAddr>) -> IpAddr {
    let from_header = limits.client_ip_header.as_ref().and_then(|name| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.rsplit(',').next())
            .and_then(|v| v.trim().parse::<IpAddr>().ok())
    });
    from_header
        .or(peer.map(|a| a.ip()))
        .unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
}

/// 推给某条连接的出站帧(经有界 mpsc 送入该连接自己的写循环)。
type OutboundTx = mpsc::Sender<Message>;

/// 一条已注册连接(桌面端或移动端槽位)。1×1 拓扑:每个槽同一时刻至多一条。
struct ConnSlot {
    generation: u64,
    tx: OutboundTx,
    /// 出站队列溢出信号:连接自己的循环收到即断开(见 [`ConnSlot::send`])
    overflow: Arc<Notify>,
}

impl ConnSlot {
    /// 往这条连接的出站队列放一帧。**不阻塞**:调用方都持着全局状态锁。
    ///
    /// 队列满 = 对端消费跟不上(弱网手机、卡住的桌面端)。策略是**断开**而不是丢帧:
    /// 镜像按 seq 连续、结构增量是差量、回执一问一答,丢掉任何一条对端都不会知道,
    /// 只会静默失真(漏一段对话、项目列表对不上、指令永远等不到回执)。断开则走的是
    /// 两端早就有的重连路径 —— 移动端重连后握手拿 presence、桌面端回发全量快照、
    /// 重新订阅拿镜像快照,状态整体自愈;桌面端重连同理。
    fn send(&self, frame: Message) {
        match self.tx.try_send(frame) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => self.overflow.notify_one(),
            // 写循环已退出:注销就在路上,这一帧本来也送不到了
            Err(TrySendError::Closed(_)) => {}
        }
    }
}

/// 把一帧写给对端,同时盯着出站队列的溢出信号。
///
/// 写本身也要和溢出赛跑:对端彻底卡死(TCP 窗口为 0)时 `send` 永远不返回,
/// 只在外层 select 里等溢出信号就永远等不到断开的那一刻。返回 `false` = 该断开了。

mod devices;

#[derive(Clone)]
pub struct RelayState {
    devices: Arc<Mutex<devices::Registry>>,
    generation_counter: Arc<AtomicU64>,
    code_ttl: Duration,
    /// 桌面端共享密钥(部署方经 `MT_RELAY_DESKTOP_KEY` 配置)。
    /// `None` = 未配置 → fail-closed,拒绝一切桌面连接。
    desktop_key: Option<Arc<String>>,
    /// 连接数 / 出站队列上限(见 [`RelayLimits`])
    limits: Arc<RelayLimits>,
    /// 在册连接计数,[`RelayState::admit`] 领名额、[`ConnPermit`] 析构归还
    conns: Arc<Mutex<ConnCounts>>,
}

impl RelayState {
    pub fn with_state_file(self, path: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        self.devices.lock().unwrap().load(path.as_ref())?;
        Ok(self)
    }

    /// 未配置桌面端密钥的实例:任何桌面连接都会被拒。
    /// 生产入口必须用 [`RelayState::with_desktop_key`] 传入实际密钥。
    pub fn new() -> Self {
        Self::with_code_ttl(PAIRING_CODE_TTL)
    }

    /// 测试用:自定义配对码有效期。
    pub fn with_code_ttl(code_ttl: Duration) -> Self {
        Self {
            devices: Arc::new(Mutex::new(devices::Registry::default())),
            generation_counter: Arc::new(AtomicU64::new(0)),
            code_ttl,
            desktop_key: None,
            limits: Arc::new(RelayLimits::default()),
            conns: Arc::new(Mutex::new(ConnCounts::default())),
        }
    }

    /// 覆盖连接数 / 出站队列上限(默认值见 [`RelayLimits::default`])。
    /// 容量类字段为 0 时按 1 处理:0 等于拒绝一切连接 / 建不出队列,不会是有意的配置。
    pub fn with_limits(mut self, limits: RelayLimits) -> Self {
        self.limits = Arc::new(RelayLimits {
            max_connections: limits.max_connections.max(1),
            max_connections_per_ip: limits.max_connections_per_ip.max(1),
            outbound_queue: limits.outbound_queue.max(1),
            ..limits
        });
        self
    }

    /// 当前生效的上限(入口据此打印启动日志)。
    pub fn limits(&self) -> &RelayLimits {
        &self.limits
    }

    /// 为一条新连接领名额:全局与该 IP 都没满才放行。
    fn admit(&self, ip: IpAddr) -> Result<ConnPermit, AdmitError> {
        let mut counts = self.conns.lock().unwrap();
        if counts.total >= self.limits.max_connections {
            return Err(AdmitError::Global);
        }
        let per_ip = counts.per_ip.get(&ip).copied().unwrap_or(0);
        if per_ip >= self.limits.max_connections_per_ip {
            return Err(AdmitError::PerIp);
        }
        counts.total += 1;
        counts.per_ip.insert(ip, per_ip + 1);
        Ok(ConnPermit {
            counts: self.conns.clone(),
            ip,
        })
    }

    /// 升级前的名额闸:解析客户端地址 → 领名额;拒绝时记一行元数据(不记地址本身)。
    fn admit_request(
        &self,
        side: &str,
        headers: &HeaderMap,
        peer: Option<Extension<ConnectInfo<SocketAddr>>>,
    ) -> Result<ConnPermit, AdmitError> {
        let peer = peer.map(|Extension(ConnectInfo(addr))| addr);
        let ip = client_ip(&self.limits, headers, peer);
        self.admit(ip).inspect_err(|reason| match reason {
            AdmitError::Global => eprintln!(
                "[relay] {side} connection rejected: global limit ({}) reached",
                self.limits.max_connections
            ),
            AdmitError::PerIp => eprintln!(
                "[relay] {side} connection rejected: per-client limit ({}) reached",
                self.limits.max_connections_per_ip
            ),
        })
    }

    /// 新建一条连接的出站队列(容量取 [`RelayLimits::outbound_queue`])。
    fn outbound_channel(&self) -> (OutboundTx, mpsc::Receiver<Message>) {
        mpsc::channel(self.limits.outbound_queue)
    }

    /// 配置桌面端共享密钥。空白字符串按"未配置"处理(避免 `MT_RELAY_DESKTOP_KEY=`
    /// 这种写法被当成"密钥就是空串"而放行任意空密钥的桌面端)。
    pub fn with_desktop_key(mut self, key: Option<String>) -> Self {
        self.desktop_key = key
            .map(|k| k.trim().to_string())
            .filter(|k| !k.is_empty())
            .map(Arc::new);
        self
    }

    /// 是否已配置桌面端密钥(入口据此打印启动日志)。
    pub fn desktop_key_configured(&self) -> bool {
        self.desktop_key.is_some()
    }

    /// 桌面端握手鉴权:先看中转有没有配密钥(未配 = 拒绝一切),再比对。
    fn authenticate_desktop(&self, presented: &str) -> Result<(), DesktopRejectReason> {
        match self.desktop_key.as_deref() {
            None => Err(DesktopRejectReason::KeyNotConfigured),
            Some(expected) if secret_eq(expected, presented) => Ok(()),
            Some(_) => Err(DesktopRejectReason::InvalidKey),
        }
    }

    fn next_generation(&self) -> u64 {
        self.generation_counter.fetch_add(1, Ordering::Relaxed) + 1
    }
}

impl Default for RelayState {
    fn default() -> Self {
        Self::new()
    }
}

/// 密钥比对:等长时不因首个差异字节提前返回。长度本身仍会泄漏(不敏感)。
fn secret_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// WebSocket 端点路由(不含 PWA 静态资源,测试直接用这个)。
pub fn app(state: RelayState) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/ws/desktop", any(desktop_ws_handler))
        .route("/ws/mobile", any(mobile_ws_handler))
        .with_state(state)
}

/// 端点路由 + PWA 静态托管:非 API 路径回退到 `pwa_dir`,未命中文件时兜底
/// index.html(SPA 路由)。移动端扫码打开的页面即由此提供。
pub fn app_with_pwa(state: RelayState, pwa_dir: &str) -> Router {
    let index = std::path::Path::new(pwa_dir).join("index.html");
    let serve = tower_http::services::ServeDir::new(pwa_dir)
        .fallback(tower_http::services::ServeFile::new(index));
    app(state).fallback_service(serve)
}

fn to_text<T: serde::Serialize>(msg: &T) -> Message {
    Message::Text(serde_json::to_string(msg).unwrap().into())
}

/// 生成不可猜测的随机 id(配对码/凭证)。uuid v4 simple 格式,32 位十六进制。
fn random_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

async fn desktop_ws_handler(
    ws: WebSocketUpgrade, State(state): State<RelayState>, headers: HeaderMap,
    peer: Option<Extension<ConnectInfo<SocketAddr>>>,
) -> Response {
    let permit = match state.admit_request("desktop", &headers, peer) {
        Ok(permit) => permit,
        Err(reason) => return reason.into_response(),
    };
    ws.max_message_size(DESKTOP_MAX_MESSAGE_BYTES).max_frame_size(DESKTOP_MAX_MESSAGE_BYTES)
        .on_upgrade(move |socket| async move {
            let _permit = permit;
            devices::desktop(socket, state).await;
        })
}

async fn mobile_ws_handler(
    ws: WebSocketUpgrade, State(state): State<RelayState>, headers: HeaderMap,
    peer: Option<Extension<ConnectInfo<SocketAddr>>>,
) -> Response {
    let permit = match state.admit_request("mobile", &headers, peer) {
        Ok(permit) => permit,
        Err(reason) => return reason.into_response(),
    };
    ws.max_message_size(MOBILE_MAX_MESSAGE_BYTES).max_frame_size(MOBILE_MAX_MESSAGE_BYTES)
        .on_upgrade(move |socket| async move {
            let _permit = permit;
            devices::mobile(socket, state).await;
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    /// 名额:同一 IP 先撞按 IP 闸,换个 IP 再撞全局闸;名额随 permit 析构归还,
    /// 计数归零的 IP 条目随之删除(map 不会被一次性的来源撑大)。
    #[test]
    fn admit_enforces_global_and_per_ip_limits_and_releases_on_drop() {
        let state = RelayState::new().with_limits(RelayLimits {
            max_connections: 3,
            max_connections_per_ip: 2,
            ..RelayLimits::default()
        });
        let (a, b) = (ip("203.0.113.1"), ip("203.0.113.2"));

        let p1 = state.admit(a).unwrap();
        let p2 = state.admit(a).unwrap();
        assert_eq!(state.admit(a).err(), Some(AdmitError::PerIp));
        let p3 = state.admit(b).unwrap();
        assert_eq!(state.admit(b).err(), Some(AdmitError::Global));

        drop(p1);
        let p4 = state.admit(a).expect("析构后名额应归还");
        drop((p2, p3, p4));

        let counts = state.conns.lock().unwrap();
        assert_eq!(counts.total, 0);
        assert!(counts.per_ip.is_empty(), "归零的 IP 条目应删除");
    }

    /// 0 不是有意的配置(等于拒绝一切连接 / 建不出队列),按 1 处理。
    #[test]
    fn zero_limits_are_clamped_to_one() {
        let state = RelayState::new().with_limits(RelayLimits {
            max_connections: 0,
            max_connections_per_ip: 0,
            client_ip_header: None,
            outbound_queue: 0,
        });
        let limits = state.limits();
        assert_eq!(
            (
                limits.max_connections,
                limits.max_connections_per_ip,
                limits.outbound_queue
            ),
            (1, 1, 1)
        );
        let _only = state.admit(ip("198.51.100.1")).unwrap();
    }

    /// 计数用的客户端地址:没配头名时只认对端地址(头里写什么都不理);配了取头的
    /// 最后一段;头缺失 / 不是 IP 时回落对端;两样都没有归 0.0.0.0 桶。
    #[test]
    fn client_ip_uses_configured_header_last_hop_then_peer() {
        let peer: SocketAddr = "10.0.0.9:5555".parse().unwrap();
        let mut limits = RelayLimits::default();
        let mut headers = HeaderMap::new();
        headers.insert("cf-connecting-ip", "198.51.100.7".parse().unwrap());
        assert_eq!(client_ip(&limits, &headers, Some(peer)), peer.ip());

        limits.client_ip_header = Some(HeaderName::from_static("cf-connecting-ip"));
        assert_eq!(client_ip(&limits, &headers, Some(peer)), ip("198.51.100.7"));

        headers.insert("cf-connecting-ip", "1.1.1.1, 198.51.100.8".parse().unwrap());
        assert_eq!(client_ip(&limits, &headers, Some(peer)), ip("198.51.100.8"));

        headers.insert("cf-connecting-ip", "garbage".parse().unwrap());
        assert_eq!(client_ip(&limits, &headers, Some(peer)), peer.ip());
        assert_eq!(
            client_ip(&limits, &HeaderMap::new(), None),
            IpAddr::V4(Ipv4Addr::UNSPECIFIED)
        );
    }

    /// 出站队列有界:满了不再增长,而是发出溢出信号让连接自行断开。
    #[tokio::test]
    async fn full_outbound_queue_signals_overflow_instead_of_growing() {
        let (tx, rx) = mpsc::channel(2);
        let overflow = Arc::new(Notify::new());
        let slot = ConnSlot {
            generation: 1,
            tx,
            overflow: overflow.clone(),
        };
        slot.send(Message::Text("a".into()));
        slot.send(Message::Text("b".into()));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), overflow.notified())
                .await
                .is_err(),
            "未满之前不该报溢出"
        );

        slot.send(Message::Text("c".into()));
        tokio::time::timeout(Duration::from_secs(1), overflow.notified())
            .await
            .expect("队列满时应发出溢出信号");
        assert_eq!(rx.len(), 2, "满了之后的帧不进队列");
    }
}
