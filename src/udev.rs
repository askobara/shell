use std::path::{Path, PathBuf};

use anyhow::{Result, anyhow};
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

async fn read_file(device: &Path, key: &str) -> Result<u32> {
    let value = fs::read_to_string(device.join(key)).await?;
    let value = value.trim().parse::<u32>()?;

    Ok(value)
}


async fn get_brightness(device: &Path) -> Result<u32> {
    read_file(device, "brightness").await
}

async fn get_max_brightness(device: &Path) -> Result<u32> {
    read_file(device, "max_brightness").await
}

async fn get_ac_status(device: &Path) ->  Result<u32>  {
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

    while let Some(event) = monitor.next().await {
        if let Ok(event) = event {
            match event.subsystem().and_then(|v| v.to_str()) {
                Some("backlight") => {
                    if let Ok(value) = get_brightness(event.device().syspath()).await {
                        tx.send(crate::Command::BrightnessChanged {
                            value,
                            device: event.device().syspath().to_string_lossy().to_string(),
                        })
                        .await?;
                    }
                },
                Some("power_supply") => {
                    if event.sysname() == "AC" {
                        if let Ok(value) = get_ac_status(event.device().syspath()).await {
                            tx.send(crate::Command::PowerSupplyChanged {
                                ac_status: value,
                            })
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
