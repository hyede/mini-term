import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import {
  clearStartError, closeHistoryView, loadSessionContent, loadSessionHistory, openHistorySession,
  openMirror, refreshSessions, resumeHistorySession, setSessionTab, useRelayStore,
} from './relay';
import { useI18nStore, useT } from './i18n';
import { Markdown } from './MirrorMarkdown';
import { StartSessionSheet } from './StartSessionSheet';
import { RenameSheet } from './RenameSheet';
import type { MobilePane } from './protocol';

const STATUS_CLASS: Record<string, string> = {
  'ai-working': 'dot-working',
  'ai-idle': 'dot-idle',
  error: 'dot-error',
};

function statusKey(status: string): string {
  switch (status) {
    case 'ai-working':
      return 'sessions.status.aiWorking';
    case 'ai-idle':
      return 'sessions.status.aiIdle';
    default:
      return 'sessions.status.error';
  }
}

function PaneRow({ pane }: { pane: MobilePane }) {
  const t = useT();
  const [renaming, setRenaming] = useState(false);
  // 黄灯(提问待答/等待授权)展示优先于常规状态:用户此刻该做的事比 AI 在不在跑重要
  const attention = pane.needsAttention === true;
  return (
    <>
      {/* 行是容器不是按钮:进镜像与改名是两个热区,按钮不能相互嵌套 */}
      <div className="pane-row">
        <button className="pane-open" onClick={() => openMirror(pane.paneId, pane.title)}>
          <span
            className={`status-dot ${attention ? 'dot-attention' : (STATUS_CLASS[pane.status] ?? 'dot-error')}`}
          />
          <span className="pane-title">{pane.title}</span>
          <span className={`pane-status${attention ? ' pane-status-attention' : ''}`}>
            {t(attention ? 'sessions.status.attention' : statusKey(pane.status))}
          </span>
          <span className="pane-chevron">›</span>
        </button>
        <button
          className="pane-rename"
          aria-label={t('sessions.rename.action')}
          title={t('sessions.rename.action')}
          onClick={() => setRenaming(true)}
        >
          ✎
        </button>
      </div>
      {renaming && (
        <RenameSheet
          paneId={pane.paneId}
          current={pane.title}
          onClose={() => setRenaming(false)}
        />
      )}
    </>
  );
}

const SESSION_AGENTS = ['claude', 'codex', 'grok', 'omp', 'pi', 'agy'];

function sessionTime(timestamp: string, lang: string) {
  const date = new Date(timestamp);
  return Number.isNaN(date.getTime()) ? '' : date.toLocaleString(lang === 'zh' ? 'zh-CN' : 'en-US');
}

function HistoryList() {
  const t = useT();
  const lang = useI18nStore((state) => state.lang);
  const projects = useRelayStore((state) => state.projects);
  const history = useRelayStore((state) => state.historyList);
  const phase = useRelayStore((state) => state.phase);
  const online = useRelayStore((state) => state.desktopOnline);
  const desktopId = useRelayStore((state) => state.selectedDesktopId);
  const [selection, setSelection] = useState(history?.projectId ?? '');
  const [agent, setAgent] = useState(history?.agent ?? '');
  const localProjects = projects.filter((project) => project.canStartSession);
  const projectId = localProjects.some((project) => project.projectId === selection)
    ? selection : localProjects[0]?.projectId ?? '';
  const ready = phase === 'connected' && online === true;
  const current = history?.projectId === projectId && history.agent === (agent || null) ? history : null;

  useEffect(() => {
    if (projectId && ready) loadSessionHistory(projectId, agent || null);
  }, [projectId, agent, ready, desktopId]);

  return (
    <div className="history-list">
      <div className="history-filters">
        <label>
          <span>{t('sessions.history.project')}</span>
          <select value={projectId} disabled={!ready || localProjects.length === 0}
            onChange={(event) => setSelection(event.target.value)}>
            {localProjects.length === 0 && <option value="">{t('sessions.history.noProjects')}</option>}
            {localProjects.map((project) => <option key={project.projectId} value={project.projectId}>{project.name}</option>)}
          </select>
        </label>
        <label>
          <span>{t('sessions.history.agent')}</span>
          <select value={agent} disabled={!ready} onChange={(event) => setAgent(event.target.value)}>
            <option value="">{t('sessions.history.allAgents')}</option>
            {SESSION_AGENTS.map((agent) => <option key={agent} value={agent}>{agent}</option>)}
          </select>
        </label>
        <button className="history-action" disabled={!ready || !projectId || current?.loading}
          onClick={() => loadSessionHistory(projectId, agent || null)}>{t('sessions.history.refresh')}</button>
      </div>
      <p className="history-hint">{t('sessions.history.localOnly')}</p>
      {current?.error && <p className="history-error" role="alert">{t(`sessions.history.error.${current.error}`)}</p>}
      {current?.sessions.map((session) => (
        <button className="history-row" key={JSON.stringify([session.agent, session.id])} disabled={!ready}
          onClick={() => openHistorySession(projectId, session)}>
          <span className="history-row-title">{session.title || t('sessions.history.untitled')}</span>
          <span className="history-row-meta">
            <span>{session.agent}{session.model ? ` · ${session.model}` : ''}</span>
            <time dateTime={session.timestamp}>{sessionTime(session.timestamp, lang)}</time>
          </span>
        </button>
      ))}
      {current?.loading && <p className="history-hint" role="status">{t('sessions.history.loading')}</p>}
      {ready && projectId && current && !current.loading && !current.error && current.sessions.length === 0
        && <p className="history-hint">{t('sessions.history.empty')}</p>}
      {!projectId && <p className="history-hint">{t('sessions.history.noProjects')}</p>}
      {current?.hasMore && <button className="mirror-load-older" disabled={!ready || current.loading}
        onClick={() => loadSessionHistory(projectId, agent || null, true)}>{t('sessions.history.more')}</button>}
    </div>
  );
}

export function HistoryView() {
  const t = useT();
  const lang = useI18nStore((state) => state.lang);
  const view = useRelayStore((state) => state.historyView);
  const online = useRelayStore((state) => state.desktopOnline);
  const starting = useRelayStore((state) => state.starting);
  const startError = useRelayStore((state) => state.startError);
  const projects = useRelayStore((state) => state.projects);
  const scroller = useRef<HTMLDivElement>(null);
  const pendingScroll = useRef<{ height: number; top: number } | null>(null);

  useLayoutEffect(() => {
    if (!scroller.current || !view || view.loading) return;
    if (pendingScroll.current) {
      scroller.current.scrollTop = pendingScroll.current.top + scroller.current.scrollHeight - pendingScroll.current.height;
      pendingScroll.current = null;
    } else if (!view.append) {
      scroller.current.scrollTop = scroller.current.scrollHeight;
    }
  }, [view?.messages, view?.loading, view?.append]);

  if (!view) return null;
  const ready = online === true;
  const projectAvailable = projects.some((project) => project.projectId === view.projectId && project.canStartSession);
  const loadOlder = () => {
    if (scroller.current) pendingScroll.current = { height: scroller.current.scrollHeight, top: scroller.current.scrollTop };
    if (!loadSessionContent(true)) pendingScroll.current = null;
  };

  return (
    <div className="mirror-view history-view">
      <div className="mirror-header">
        <button className="mirror-back" disabled={!!starting} onClick={closeHistoryView}>{t('sessions.history.back')}</button>
        <h2 className="mirror-title">{view.session.title || t('sessions.history.untitled')}</h2>
        <button className="history-action" disabled={!ready || !projectAvailable || view.loading || !!starting}
          onClick={() => loadSessionContent()}>{t('sessions.history.refresh')}</button>
      </div>
      {!ready && <div className="offline-banner">{t('sessions.offlineBanner')}</div>}
      <div className="mirror-scroll" ref={scroller} aria-busy={view.loading}>
        {!projectAvailable && <p className="history-error" role="alert">{t('sessions.history.error.projectNotFound')}</p>}
        {view.hasMore && <button className="mirror-load-older" disabled={!ready || view.loading}
          onClick={loadOlder}>{t('mirror.loadOlder')}</button>}
        {view.error && <p className="history-error" role="alert">{t(`sessions.history.error.${view.error}`)}</p>}
        {view.loading && <p className="history-hint" role="status">{t('sessions.history.loading')}</p>}
        {!view.loading && !view.error && view.messages.length === 0 && <p className="history-hint">{t('sessions.history.emptyContent')}</p>}
        {view.messages.map((message) => (
          <article className={`mirror-msg ${message.source === 'assistant' ? 'from-assistant' : 'from-input'}`} key={message.seq}>
            <div className="mirror-msg-source">
              <span>{message.source === 'desktop' ? t('sessions.history.user') : message.source === 'assistant'
                ? t('mirror.source.assistant') : t('sessions.history.tool')}</span>
              <time className="mirror-msg-time" dateTime={message.timestamp}>{sessionTime(message.timestamp, lang)}</time>
            </div>
            <div className="mirror-msg-body markdown"><Markdown content={message.content} /></div>
          </article>
        ))}
      </div>
      <div className="history-footer">
        {startError && <p className="history-error" role="alert">{t(`start.error.${startError}`)}</p>}
        <p className="history-hint">{t(view.session.canResume ? 'sessions.history.resumeHint' : 'sessions.history.resumeUnsupported')}</p>
        <button className="rename-confirm" disabled={!ready || !projectAvailable || !!starting || !view.session.canResume || view.error === 'sessionNotFound'}
          onClick={resumeHistorySession}>{t(starting ? 'sessions.history.resuming' : 'sessions.history.resume')}</button>
      </div>
    </div>
  );
}

/** 活跃 AI 会话列表:按项目分组;桌面端离线时置灰不可交互。 */
export function SessionList() {
  const t = useT();
  const tab = useRelayStore((state) => state.sessionTab);
  const projects = useRelayStore((s) => s.projects);
  const launchers = useRelayStore((s) => s.launchers);
  const desktopOnline = useRelayStore((s) => s.desktopOnline);
  const starting = useRelayStore((s) => s.starting);
  const startError = useRelayStore((s) => s.startError);
  const [sheetOpen, setSheetOpen] = useState(false);
  const offline = desktopOnline === false;

  // ── 下拉刷新 ──
  // 列表本身是实时同步的，但「怀疑它卡住了」时用户需要一个能自己动手的动作。
  const [pull, setPull] = useState(0);
  const [refreshing, setRefreshing] = useState(false);
  const pullStart = useRef<number | null>(null);
  const PULL_TRIGGER = 64;

  const onTouchStart = (e: React.TouchEvent) => {
    // 只在真正滚到顶时才接管手势，否则会和正常滚动打架
    const scroller = e.currentTarget.parentElement;
    if (scroller && scroller.scrollTop > 0) return;
    if (refreshing) return;
    pullStart.current = e.touches[0].clientY;
  };

  const onTouchMove = (e: React.TouchEvent) => {
    if (pullStart.current === null) return;
    const delta = e.touches[0].clientY - pullStart.current;
    if (delta <= 0) {
      setPull(0);
      return;
    }
    // 阻尼：越拉越沉，避免手指一滑就整屏位移
    setPull(Math.min(delta * 0.5, PULL_TRIGGER + 20));
  };

  const onTouchEnd = () => {
    if (pullStart.current === null) return;
    const shouldRefresh = pull >= PULL_TRIGGER;
    pullStart.current = null;
    setPull(0);
    if (!shouldRefresh) return;
    if (!refreshSessions()) return;
    setRefreshing(true);
    // 重连握手 + 全量快照通常在 1s 内；这里只是指示器的收尾，不是真等待
    setTimeout(() => setRefreshing(false), 1200);
  };

  // 失败提示展示 6s 后自动消失(超时文案偏长,给足阅读时间)
  useEffect(() => {
    if (!startError) return;
    const timer = setTimeout(clearStartError, 6000);
    return () => clearTimeout(timer);
  }, [startError]);

  // 快照含全部项目(发起弹层要用),首页仍只渲染有活跃会话的那些
  const active = projects.filter((p) => p.panes.length > 0);

  // + 按钮不可用的原因,按优先级取第一条;null = 可用
  const disabledReason = offline
    ? 'offline'
    : launchers.length === 0
      ? 'noLaunchers'
      : starting
        ? 'starting'
        : null;

  return (
    <div className="session-list">
      <div className="session-tabs" role="tablist" aria-label={t('sessions.history.tabs')}>
        <button role="tab" aria-selected={tab === 'active'} disabled={!!starting}
          onClick={() => setSessionTab('active')}>{t('sessions.history.active')}</button>
        <button role="tab" aria-selected={tab === 'history'} disabled={!!starting}
          onClick={() => setSessionTab('history')}>{t('sessions.history.title')}</button>
      </div>
      {offline && (
        <div className="offline-banner">
          <div className="offline-title">{t('sessions.offlineBanner')}</div>
          <div className="offline-hint">{t('sessions.offlineHint')}</div>
        </div>
      )}
      {starting && (
        <div className="start-banner">{t('start.starting', { project: starting.projectName })}</div>
      )}
      {startError && (
        <div className="start-banner start-banner--error">{t(`start.error.${startError}`)}</div>
      )}
      {tab === 'history' && <HistoryList />}
      {tab === 'active' && <>
      <div
        className="pull-indicator"
        style={{ height: refreshing ? 36 : pull }}
        aria-hidden={!refreshing && pull === 0}
      >
        {refreshing ? (
          <><span className="spinner" />{t('sessions.refreshing')}</>
        ) : pull >= PULL_TRIGGER ? (
          t('sessions.releaseToRefresh')
        ) : pull > 0 ? (
          t('sessions.pullToRefresh')
        ) : null}
      </div>

      <div
        className={`session-body ${offline ? 'inert' : ''}`}
        onTouchStart={onTouchStart}
        onTouchMove={onTouchMove}
        onTouchEnd={onTouchEnd}
        onTouchCancel={onTouchEnd}
      >
        {active.length === 0 ? (
          <div className="sessions-empty">
            <div className="sessions-empty-title">{t('sessions.empty')}</div>
            <div className="sessions-empty-hint">{t('sessions.emptyHint')}</div>
          </div>
        ) : (
          active.map((project) => (
            <section key={project.projectId} className="project-card">
              <h2 className="project-name">{project.name}</h2>
              {project.panes.map((pane) => (
                <PaneRow key={pane.paneId} pane={pane} />
              ))}
            </section>
          ))
        )}
      </div>

      </>}

      <button
        className="fab"
        disabled={disabledReason !== null}
        title={disabledReason ? t(`start.disabled.${disabledReason}`) : t('start.fab')}
        aria-label={t('start.fab')}
        onClick={() => setSheetOpen(true)}
      >
        +
      </button>
      {disabledReason && disabledReason !== 'starting' && (
        <div className="fab-hint">{t(`start.disabled.${disabledReason}`)}</div>
      )}

      {sheetOpen && <StartSessionSheet onClose={() => setSheetOpen(false)} />}
    </div>
  );
}
