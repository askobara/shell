use std::{
    path::{Path, PathBuf},
    str::FromStr,
};

use anyhow::Result;
use log::debug;
use tokio::{
    fs,
    io::{Interest, unix::AsyncFd},
    sync::mpsc::Sender,
};
use tokio_udev::AsyncMonitorSocket;
use udev::{Device, MonitorBuilder, MonitorSocket};

use futures_util::stream::StreamExt;
use std::convert::TryInto;

async fn read_file<T>(device: &Path, key: &str) -> Result<T>
where
    T: FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    let value = fs::read_to_string(device.join(key)).await?;
    let value = value.trim().parse::<T>()?;

    Ok(value)
}

async fn get_brightness(device: &Path) -> Result<f32> {
    read_file(device, "brightness").await
}

async fn get_max_brightness(device: &Path) -> Result<f32> {
    read_file(device, "max_brightness").await
}

async fn get_ac_status(device: &Path) -> Result<u32> {
    read_file(device, "online").await
}

pub async fn events(tx: &Sender<crate::Command>) -> Result<()> {
    // let mut enumerator = udev::Enumerator::new()?;
    // enumerator.match_subsystem("backlight")?;
    //
    // let device = enumerator
    //     .scan_devices()?
    //     .into_iter()
    //     .find(|d| d.subsystem().and_then(|s| s.to_str()) == Some("backlight"))
    //     .as_ref()
    //     .map(|d| d.syspath().to_owned());
    //
    // if let Some(ref path) = device {
    //     let brightness = get_brightness(&path).await?;
    //     println!("{brightness}");
    // }

    let builder = MonitorBuilder::new()?
        .match_subsystem("power_supply")?
        // .match_subsystem("bluetooth")?
        .match_subsystem("backlight")?;

    let mut monitor: AsyncMonitorSocket = builder.listen()?.try_into()?;

    let mut backlight_max: Option<f32> = None;

    while let Some(event) = monitor.next().await {
        if let Ok(event) = event {
            match event.subsystem().and_then(|v| v.to_str()) {
                Some("backlight") => {
                    if backlight_max.is_none() {
                        backlight_max = get_max_brightness(event.device().syspath()).await.ok();
                    }

                    let value = get_brightness(event.device().syspath()).await.ok();

                    if let Some(result) = value.zip(backlight_max).map(|(v, max)| v / max * 100.0) {
                        tx.send(crate::Command::BrightnessChanged {
                            value: result as u32,
                            device: event.device().syspath().to_string_lossy().to_string(),
                        })
                        .await?;
                    }
                }
                Some("power_supply") => {
                    if event.sysname() == "AC" {
                        if let Ok(value) = get_ac_status(event.device().syspath()).await {
                            tx.send(crate::Command::PowerSupplyChanged { ac_status: value })
                                .await?;
                        }
                    }
                }
                _ => {
                    dbg!(&event);
                }
            }
        }
    }

    Ok(())
}
