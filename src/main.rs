use std::{collections::HashMap};
use std::{env, fmt};
use std::error::Error;
use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt, Interest};
use tokio::net::UnixStream;
use log::debug;

use anyhow::Result;

use zbus::{Connection, proxy, zvariant::Value};

mod pulseaudio;
mod hyprland;

enum Icon {
    Keyboard,
    Sound,
    Muted,
    DotFull,
    DotOutline,
    Microphone,
    Brightness,
}

impl Icon {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Icon::Keyboard => "",
            Icon::Sound => "",
            Icon::Muted => "󰖁",
            Icon::DotFull => "●",
            Icon::DotOutline => "○",
            Icon::Microphone => "",
            Icon::Brightness => "󰃠",
        }
    }
}

impl fmt::Display for Icon {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
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

struct Notif<'p> {
    name: String,
    proxy: NotificationsProxy<'p>,
}

impl<'p> Notif<'p> {
    async fn new(name: &str, dbus: &Connection) -> Result<Self> {

        Ok(Self {
            name: name.to_owned(),
            proxy: NotificationsProxy::new(&dbus).await?,
        })
    }

    async fn send_notif(&self, msg: &str) -> Result<()> {
        self.proxy
            .notify(
                &self.name,
                0,
                "",
                &msg,
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

    pub async fn layout_change(&self, layout_name: &str) -> Result<()> {
        self.send_notif(&format!("{} {}", Icon::Keyboard, &layout_name.to_lowercase()[..2])).await
    }

    pub async fn workspace_change(&self, workspace_id: &str, workspaces: &[hyprland::Workspace]) -> Result<()> {
        let id: u32 = workspace_id.parse()?;
        let msg: Vec<&str> = workspaces.iter()
            .map(|ws| {if ws.id == id { Icon::DotFull } else { Icon::DotOutline }}.as_str())
            .collect();

        self.send_notif(&msg.join(" ")).await
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let dbus = Connection::session().await?;
    let n = Notif::new("my-app", &dbus).await?;

    let signature = env::var("HYPRLAND_INSTANCE_SIGNATURE")?;
    let runtime_dir = env::var("XDG_RUNTIME_DIR")?;

    let ipc_socket = format!("{runtime_dir}/hypr/{signature}/.socket.sock");
    let events_socket = format!("{runtime_dir}/hypr/{signature}/.socket2.sock");

    hyprland::events(&ipc_socket, &events_socket, &n).await?;

    Ok(())
}
