use log::debug;
use std::{env, io};
use tokio::io::{AsyncReadExt, AsyncWriteExt, Interest};
use tokio::net::UnixStream;
use tokio::sync::mpsc::Sender;

use anyhow::Result;

// {
//     "id": 9,
//     "name": "9",
//     "monitor": "eDP-1",
//     "monitorID": 0,
//     "windows": 1,
//     "hasfullscreen": false,
//     "lastwindow": "0x558c38585fb0",
//     "lastwindowtitle": "Inbox - skobara.arthur@gmail.com - Mozilla Thunderbird",
//     "ispersistent": true,
//     "tiledLayout": "monocle"
// }

pub async fn events(tx: &Sender<crate::Command>) -> Result<()> {
    let signature = env::var("HYPRLAND_INSTANCE_SIGNATURE")?;
    let runtime_dir = env::var("XDG_RUNTIME_DIR")?;

    let ipc_socket_path = format!("{runtime_dir}/hypr/{signature}/.socket.sock");
    let events_socket_path = format!("{runtime_dir}/hypr/{signature}/.socket2.sock");

    let events = UnixStream::connect(&events_socket_path).await?;

    loop {
        let ready = events.ready(Interest::READABLE).await?;

        if ready.is_readable() {
            let mut data = vec![0; 1024];
            // Try to read data, this may still fail with `WouldBlock`
            // if the readiness event is a false positive.
            match events.try_read(&mut data) {
                Ok(n) => {
                    let lines = str::from_utf8(&data[..n])?.lines();

                    for line in lines {
                        // data format is: EVENT_NAME>>DATA
                        if let Some((event_name, payload)) = line.split_once(">>") {
                            match event_name {
                                "activelayout" => {
                                    if let Some((_kb_name, layout_name)) = payload.split_once(',') {
                                        tx.send(crate::Command::KeyboardLayout {
                                            name: layout_name.to_string(),
                                        })
                                        .await?;
                                    }
                                }
                                "workspacev2" => {
                                    if let Some((id, _name)) = payload.split_once(',') {
                                        let mut ipc = UnixStream::connect(&ipc_socket_path).await?;
                                        ipc.write_all(b"j/workspaces").await?;
                                        let mut buffer = Vec::new();
                                        ipc.read_to_end(&mut buffer).await?;
                                        let workspaces: Vec<crate::Workspace> =
                                            serde_json::from_slice(&buffer)?;

                                        tx.send(crate::Command::ActiveWorkspace {
                                            id: id.to_string(),
                                            workspaces,
                                        })
                                        .await?;
                                    }
                                }
                                _ => debug!("incoming {event_name}>>{payload}"),
                            }
                        }
                    }
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    continue;
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }
    }
}
