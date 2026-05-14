use serde::{Deserialize, Serialize};
use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt, Interest};
use tokio::net::UnixStream;
use log::debug;

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

#[derive(Serialize, Deserialize)]
pub struct Workspace {
    pub id: u32,
    pub name: String,
}

pub async fn events(ipc_path: &str, events_path: &str, notif: &crate::Notif<'_>) -> Result<()> {
    let events = UnixStream::connect(events_path).await?;

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
                                        notif.layout_change(layout_name).await?;
                                    }
                                },
                                "workspacev2" => {
                                    if let Some((id, _name)) = payload.split_once(',') {
                                        let mut ipc = UnixStream::connect(ipc_path).await?;
                                        ipc.write_all(b"j/workspaces").await?;
                                        let mut buffer = Vec::new();
                                        ipc.read_to_end(&mut buffer).await?;
                                        let workspaces: Vec<Workspace> = serde_json::from_slice(&buffer)?;

                                        notif.workspace_change(id, &workspaces).await?;
                                    }
                                }
                                _ => debug!("incoming {event_name}>>{payload}")
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
