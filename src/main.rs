use log::debug;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

use anyhow::Result;

use tokio::sync::mpsc::{self, Receiver};
use zbus::{Connection, proxy, zvariant::Value};

mod hyprland;
mod pulseaudio;
mod udev;

enum Icon {
    Keyboard,
    Volume,
    Muted,
    DotFull,
    DotOutline,
    Microphone,
    Brightness,
    PowerPlugOn,
    PowerPlugOff,
}

impl Icon {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Icon::Keyboard => "",
            Icon::Volume => "",
            Icon::Muted => "󰖁",
            // Icon::DotFull => "●",
            // Icon::DotOutline => "○",
            // Icon::DotFull => "",
            // Icon::DotOutline => "",
            Icon::DotFull => "",
            Icon::DotOutline => "",
            Icon::Microphone => "",
            Icon::Brightness => "󰃠",
            Icon::PowerPlugOn => "󰚥",
            Icon::PowerPlugOff => "󰚦",
        }
    }
}

impl fmt::Display for Icon {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug)]
enum Command {
    KeyboardLayout {
        name: String,
    },
    ActiveWorkspace {
        id: String,
        workspaces: Vec<Workspace>,
    },
    Volume {
        name: Option<String>,
        value: String,
        mute: bool,
    },
    Source {
        name: Option<String>,
        ports: Vec<String>,
    },
    BrightnessChanged {
        value: u32,
        device: String,
    },
    PowerSupplyChanged {
        ac_status: u32,
    },
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Workspace {
    pub id: u32,
    pub name: Option<String>,
}

#[proxy(
    default_service = "org.freedesktop.Notifications",
    default_path = "/org/freedesktop/Notifications"
)]
trait Notifications {
    /// Call the org.freedesktop.Notifications.Notify D-Bus method
    fn notify(
        &self,
        app_name: &str,
        replaces_id: u32,
        app_icon: &str,
        summary: &str,
        body: &str,
        actions: &[&str],
        hints: HashMap<&str, &Value<'_>>,
        expire_timeout: i32,
    ) -> zbus::Result<u32>;
}

struct DbusManager<'p> {
    name: String,
    dbus: &'p Connection,
    proxy: NotificationsProxy<'p>,
}

impl<'p> DbusManager<'p> {
    async fn new(name: &str, dbus: &'p Connection) -> Result<Self> {
        Ok(Self {
            name: name.to_owned(),
            dbus,
            proxy: NotificationsProxy::new(dbus).await?,
        })
    }

    async fn run(&self, rx: &mut Receiver<Command>) -> Result<()> {
        while let Some(message) = rx.recv().await {
            match message {
                Command::ActiveWorkspace { id, workspaces } => {
                    self.workspace_change(&id, &workspaces).await?
                }
                Command::KeyboardLayout { name } => {
                    self.send_notification(&format!(
                        "{} {}",
                        Icon::Keyboard,
                        &name.to_lowercase()[..2]
                    ))
                    .await?;
                }
                Command::Volume {
                    name: _,
                    value,
                    mute,
                } => {
                    if mute {
                        self.send_notification(Icon::Muted.as_str()).await?;
                    } else {
                        self.send_notification(&format!("{} {value}", Icon::Volume))
                            .await?;
                    }
                }
                Command::BrightnessChanged { value, device: _ } => {
                    self.send_notification(&format!("{} {}%", Icon::Brightness, value,))
                        .await?;
                }
                Command::PowerSupplyChanged { ac_status } => {
                    self.send_notification(&format!(
                        "{}",
                        if ac_status == 1 {
                            Icon::PowerPlugOn
                        } else {
                            Icon::PowerPlugOff
                        },
                    ))
                    .await?;
                }
                _ => {
                    debug!("GOT = {:?}", message);
                }
            }
        }

        Ok(())
    }

    async fn send_notification(&self, msg: &str) -> Result<()> {
        self.proxy
            .notify(
                &self.name,
                0,
                "",
                msg,
                "",
                &[],
                HashMap::from([
                    ("category", &Value::from("osd")),
                    ("x-canonical-private-synchronous", &Value::from("osd")),
                ]),
                700,
            )
            .await?;

        Ok(())
    }

    async fn workspace_change(&self, workspace_id: &str, workspaces: &[Workspace]) -> Result<()> {
        let id: u32 = workspace_id.parse()?;
        let msg: Vec<&str> = workspaces
            .iter()
            .map(|ws| {
                {
                    if ws.id == id {
                        Icon::DotFull
                    } else {
                        Icon::DotOutline
                    }
                }
                .as_str()
            })
            .collect();

        self.send_notification(&msg.join(" ")).await
    }

    pub async fn awesomewm_eval(&self, code: &str) -> Result<zbus::Message> {
        self.dbus
            .call_method(
                Some("org.awesomewm.awful"),
                "/",
                Some("org.awesomewm.awful.Remote"),
                "Eval",
                &code,
            )
            .await
            .map_err(anyhow::Error::from)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    // Create a new channel with a capacity of at most 32.
    let (tx, mut rx) = mpsc::channel::<Command>(32);
    let tx2 = tx.clone();
    let tx3 = tx.clone();

    let dbus = Connection::session().await?;
    let manager = DbusManager::new("my-app", &dbus).await?;

    tokio::task::spawn_blocking(move || {
        let _ = pulseaudio::events(tx);
    });

    tokio::spawn(async move {
        let _ = hyprland::events(&tx2).await;
    });

    tokio::spawn(async move {
        let _ = udev::events(&tx3).await;
    });

    manager.run(&mut rx).await
}
