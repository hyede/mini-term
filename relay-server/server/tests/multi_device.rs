use std::net::SocketAddr;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use mt_relay_protocol::{DesktopToRelay, RelayToDesktop, PROTOCOL_VERSION};
use mt_relay_server::{app, RelayState};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

type Client = WebSocketStream<MaybeTlsStream<TcpStream>>;
const KEY: &str = "personal-device-group";

async fn spawn_relay(state: RelayState) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app(state.with_desktop_key(Some(KEY.into()))))
            .await.unwrap();
    });
    addr
}

async fn send(client: &mut Client, value: impl serde::Serialize) {
    client.send(Message::Text(serde_json::to_string(&value).unwrap().into()))
        .await.unwrap();
}

async fn recv(client: &mut Client) -> serde_json::Value {
    loop {
        let message = tokio::time::timeout(Duration::from_secs(3), client.next())
            .await.expect("relay response timed out").expect("connection closed")
            .expect("websocket failed");
        match message {
            Message::Text(text) => return serde_json::from_str(&text).unwrap(),
            Message::Close(_) => panic!("device was unexpectedly replaced"),
            _ => {}
        }
    }
}

async fn desktop(addr: SocketAddr, id: &str) -> Client {
    let (mut client, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws/desktop"))
        .await.unwrap();
    send(&mut client, serde_json::json!({
        "type": "hello", "protocolVersion": PROTOCOL_VERSION, "desktopKey": KEY,
        "desktopId": id, "desktopName": id, "deviceKey": format!("secret-{id}")
    })).await;
    assert_eq!(recv(&mut client).await["type"], "helloAck");
    assert_eq!(recv(&mut client).await["type"], "pairingUpdate");
    client
}

async fn until(client: &mut Client, kind: &str) -> serde_json::Value {
    for _attempt in 0..30 {
        let value = recv(client).await;
        if value["type"] == kind { return value; }
    }
    panic!("expected {kind}");
}

async fn phone(addr: SocketAddr, desktop: &mut Client, device_name: &str) -> (Client, String) {
    send(desktop, DesktopToRelay::RequestPairingCode).await;
    let code = until(desktop, "pairingCode").await;
    let (mut client, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws/mobile"))
        .await.unwrap();
    send(&mut client, serde_json::json!({
        "type": "hello", "protocolVersion": PROTOCOL_VERSION,
        "pairingCode": code["code"], "multiDevice": true, "deviceName": device_name,
    })).await;
    let ack = recv(&mut client).await;
    assert_eq!(ack["type"], "helloAck");
    let credential = ack["credential"].as_str().unwrap().to_string();
    assert_eq!(recv(&mut client).await["type"], "desktopList");
    assert_eq!(recv(&mut client).await["desktopOnline"], true);
    (client, credential)
}

#[tokio::test]
async fn phones_share_mirror_without_unsubscribing_each_other() {
    let addr = spawn_relay(RelayState::new()).await;
    let mut desktop = desktop(addr, "desktop").await;
    let (mut first, _) = phone(addr, &mut desktop, "first").await;
    let (mut second, _) = phone(addr, &mut desktop, "second").await;
    send(&mut first, serde_json::json!({"type":"subscribePane", "paneId":"pane"})).await;
    until(&mut desktop, "subscribePane").await;
    send(&mut second, serde_json::json!({"type":"subscribePane", "paneId":"pane"})).await;
    until(&mut desktop, "mirrorSnapshotRequest").await;
    send(&mut desktop, serde_json::json!({
        "type":"mirrorSnapshot", "paneId":"pane", "messages":[], "hasMore":false,
    })).await;
    assert_eq!(until(&mut first, "mirrorSnapshot").await["desktopId"], "desktop");
    assert_eq!(until(&mut second, "mirrorSnapshot").await["desktopId"], "desktop");
    send(&mut first, serde_json::json!({"type":"unsubscribePane", "paneId":"pane"})).await;
    assert!(tokio::time::timeout(Duration::from_millis(100), desktop.next()).await.is_err());
    send(&mut second, serde_json::json!({"type":"unsubscribePane", "paneId":"pane"})).await;
    assert_eq!(until(&mut desktop, "unsubscribePane").await["paneId"], "pane");
}

#[tokio::test]
async fn history_replies_return_only_to_the_requesting_phone() {
    let addr = spawn_relay(RelayState::new()).await;
    let mut desktop = desktop(addr, "desktop").await;
    let (mut first, _) = phone(addr, &mut desktop, "first").await;
    let (mut second, _) = phone(addr, &mut desktop, "second").await;
    for client in [&mut first, &mut second] {
        send(client, serde_json::json!({"type":"subscribePane", "paneId":"pane"})).await;
    }
    until(&mut desktop, "mirrorSnapshotRequest").await;
    send(&mut first, serde_json::json!({"type":"requestMirrorHistory", "paneId":"pane", "beforeSeq":10})).await;
    let first_request = until(&mut desktop, "requestMirrorHistory").await;
    send(&mut second, serde_json::json!({"type":"requestMirrorHistory", "paneId":"pane", "beforeSeq":20})).await;
    let second_request = until(&mut desktop, "requestMirrorHistory").await;
    assert_ne!(first_request["requestId"], second_request["requestId"]);
    for (request, has_more) in [(&second_request, false), (&first_request, true)] {
        send(&mut desktop, serde_json::json!({
            "type":"mirrorHistory", "paneId":"pane", "messages":[], "hasMore":has_more,
            "requestId":request["requestId"],
        })).await;
    }
    assert_eq!(until(&mut first, "mirrorHistory").await["hasMore"], true);
    assert_eq!(until(&mut second, "mirrorHistory").await["hasMore"], false);
}

#[tokio::test]
async fn saved_session_replies_are_private_and_request_ids_are_restored() {
    let addr = spawn_relay(RelayState::new()).await;
    let mut desktop = desktop(addr, "desktop").await;
    let (mut first, _) = phone(addr, &mut desktop, "first").await;
    let (mut second, _) = phone(addr, &mut desktop, "second").await;
    until(&mut desktop, "sessionsSnapshotRequest").await;
    for (request_type, response_type) in [
        ("requestSessionList", "sessionList"),
        ("requestSessionContent", "sessionContent"),
        ("resumeAiSession", "startSessionReceipt"),
    ] {
        for client in [&mut first, &mut second] {
            send(client, serde_json::json!({
                "type":request_type, "requestId":"same-id", "projectId":"p1",
                "agent":"codex", "sessionId":"s1", "offset":0, "beforeSeq":null,
            })).await;
        }
        let first_request = until(&mut desktop, request_type).await;
        let second_request = until(&mut desktop, request_type).await;
        assert_ne!(first_request["requestId"], second_request["requestId"]);
        send(&mut desktop, serde_json::json!({
            "type":response_type, "requestId":second_request["requestId"], "projectId":"p1",
            "agent":"codex", "sessionId":"s1", "sessions":[], "messages":[],
            "hasMore":false, "reason":null, "ok":true, "paneId":"pane-2",
        })).await;
        assert_eq!(until(&mut second, response_type).await["requestId"], "same-id");
        assert!(tokio::time::timeout(Duration::from_millis(100), first.next()).await.is_err());
        send(&mut desktop, serde_json::json!({
            "type":response_type, "requestId":first_request["requestId"], "projectId":"p1",
            "agent":"codex", "sessionId":"s1", "sessions":[], "messages":[],
            "hasMore":false, "reason":null, "ok":true, "paneId":"pane-1",
        })).await;
        assert_eq!(until(&mut first, response_type).await["requestId"], "same-id");
    }
}

#[tokio::test]
async fn saved_sessions_fail_immediately_when_the_desktop_is_offline() {
    let addr = spawn_relay(RelayState::new()).await;
    let mut desktop = desktop(addr, "desktop").await;
    let (mut mobile, _) = phone(addr, &mut desktop, "phone").await;
    desktop.close(None).await.unwrap();
    assert_eq!(until(&mut mobile, "presence").await["desktopOnline"], false);
    for (request_type, response_type) in [
        ("requestSessionList", "sessionList"),
        ("requestSessionContent", "sessionContent"),
        ("resumeAiSession", "startSessionReceipt"),
    ] {
        send(&mut mobile, serde_json::json!({
            "type":request_type, "requestId":"offline-id", "projectId":"p1", "agent":"codex",
            "sessionId":"s1", "offset":0, "beforeSeq":null,
        })).await;
        let response = until(&mut mobile, response_type).await;
        assert_eq!(response["requestId"], "offline-id");
        assert_eq!(response["reason"], "desktopOffline");
    }
}

#[tokio::test]
async fn selected_desktop_routes_colliding_pane_and_command_ids_independently() {
    let addr = spawn_relay(RelayState::new()).await;
    let mut first_desktop = desktop(addr, "first").await;
    let mut second_desktop = desktop(addr, "second").await;
    let (mut first_phone, _) = phone(addr, &mut first_desktop, "first phone").await;
    let (mut second_phone, _) = phone(addr, &mut second_desktop, "second phone").await;
    for client in [&mut first_phone, &mut second_phone] {
        send(client, serde_json::json!({
            "type":"mobileCommand", "paneId":"same-pane", "commandId":"same-command", "text":"hello",
        })).await;
    }
    let first = until(&mut first_desktop, "mobileCommand").await;
    let second = until(&mut second_desktop, "mobileCommand").await;
    assert_ne!(first["commandId"], second["commandId"]);
    for (client, command) in [(&mut first_desktop, first), (&mut second_desktop, second)] {
        send(client, serde_json::json!({
            "type":"commandReceipt", "paneId":"same-pane", "commandId":command["commandId"], "ok":true,
        })).await;
    }
    assert_eq!(until(&mut first_phone, "commandReceipt").await["desktopId"], "first");
    assert_eq!(until(&mut second_phone, "commandReceipt").await["desktopId"], "second");
    send(&mut first_phone, serde_json::json!({"type":"selectDesktop", "desktopId":"second"})).await;
    assert_eq!(until(&mut first_phone, "desktopList").await["selectedDesktopId"], "second");
}

#[tokio::test]
async fn control_is_released_without_interrupting_other_viewers() {
    let addr = spawn_relay(RelayState::new()).await;
    let mut desktop = desktop(addr, "desktop").await;
    let (mut first, _) = phone(addr, &mut desktop, "first").await;
    let (mut second, _) = phone(addr, &mut desktop, "second").await;
    let command = serde_json::json!({"type":"mobileCommand", "paneId":"pane", "commandId":"cmd", "text":"hello"});
    send(&mut first, &command).await;
    until(&mut desktop, "mobileCommand").await;
    send(&mut second, &command).await;
    assert_eq!(until(&mut second, "commandReceipt").await["reason"], "controlBusy");
    send(&mut first, serde_json::json!({"type":"releaseControl", "paneId":"pane"})).await;
    until(&mut first, "controlReleased").await;
    send(&mut second, &command).await;
    until(&mut desktop, "mobileCommand").await;
}

#[tokio::test]
async fn restart_retains_authorization_but_never_message_bodies() {
    let root = std::env::temp_dir().join(format!("mt-relay-devices-{}", uuid::Uuid::new_v4()));
    let path = root.join("devices.json");
    let addr = spawn_relay(RelayState::new().with_state_file(&path).unwrap()).await;
    let mut desktop = desktop(addr, "desktop").await;
    let (mut phone, credential) = phone(addr, &mut desktop, "phone").await;
    let canary = "conversation-body-must-never-be-persisted";
    send(&mut phone, serde_json::json!({"type":"mobileCommand", "paneId":"pane", "commandId":"cmd", "text":canary})).await;
    until(&mut desktop, "mobileCommand").await;
    assert!(!std::fs::read_to_string(&path).unwrap().contains(canary));
    let restarted = spawn_relay(RelayState::new().with_state_file(&path).unwrap()).await;
    let (mut reconnected, _) = tokio_tungstenite::connect_async(format!("ws://{restarted}/ws/mobile"))
        .await.unwrap();
    send(&mut reconnected, serde_json::json!({
        "type":"hello", "protocolVersion":PROTOCOL_VERSION, "credential":credential, "multiDevice":true,
    })).await;
    assert_eq!(recv(&mut reconnected).await["type"], "helloAck");
    let list = recv(&mut reconnected).await;
    assert_eq!(list["devices"][0]["desktopId"], "desktop");
    assert_eq!(list["devices"][0]["online"], false);
    assert!(root.file_name().unwrap().to_string_lossy().starts_with("mt-relay-devices-"));
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn different_desktops_remain_connected_to_one_relay() {
    let addr = spawn_relay(RelayState::new()).await;
    let mut first = desktop(addr, "first").await;
    let mut second = desktop(addr, "second").await;
    send(&mut first, DesktopToRelay::RequestPairingCode).await;
    assert_eq!(recv(&mut first).await["type"], "pairingCode");
    send(&mut second, DesktopToRelay::RequestPairingCode).await;
    let response: RelayToDesktop = serde_json::from_value(recv(&mut second).await).unwrap();
    assert!(matches!(response, RelayToDesktop::PairingCode { .. }));
}
