use anyhow::{Result, anyhow};
use log::debug;
use std::{cell::RefCell, rc::Rc};
use tokio::sync::mpsc::Sender;

use pulse::{
    callbacks::ListResult,
    context::{
        Context, FlagSet, State,
        introspect::{ServerInfo, SinkInfo, SourceInfo},
        subscribe::{Facility, InterestMaskSet, Operation},
    },
    mainloop::standard::{IterateResult, Mainloop},
    proplist::Proplist,
};
use std::ops::Deref;

const PA_NAME: &str = "pa-events-watcher";

pub struct EventsWatcher {
    mainloop: Rc<RefCell<Mainloop>>,
    context: Rc<RefCell<Context>>,
}

impl EventsWatcher {
    fn new() -> Result<EventsWatcher> {
        let mut proplist = Proplist::new().unwrap();

        proplist
            .set_str(pulse::proplist::properties::APPLICATION_NAME, PA_NAME)
            .unwrap();

        let mainloop = match Mainloop::new() {
            Some(mainloop) => Rc::new(RefCell::new(mainloop)),
            None => return Err(anyhow!("Failed to create mainloop")),
        };

        let context =
            match Context::new_with_proplist(mainloop.borrow().deref(), PA_NAME, &proplist) {
                Some(context) => Rc::new(RefCell::new(context)),
                None => return Err(anyhow!("Failed to create new context")),
            };

        context
            .borrow_mut()
            .connect(None, FlagSet::NOFLAGS, None)
            .map_err(anyhow::Error::from)?;

        loop {
            match mainloop.borrow_mut().iterate(true) {
                IterateResult::Quit(_) | IterateResult::Err(_) => {
                    return Err(anyhow!("Iterate state was not success, quitting..."));
                }
                IterateResult::Success(_) => {}
            }

            match context.borrow().get_state() {
                State::Ready => {
                    break;
                }
                State::Failed | State::Terminated => {
                    return Err(anyhow!("Context state failed/terminated, quitting..."));
                }
                _ => {}
            }
        }

        Ok(EventsWatcher { mainloop, context })
    }
}

impl Drop for EventsWatcher {
    fn drop(&mut self) {
        self.context.borrow_mut().disconnect();
        self.mainloop.borrow_mut().quit(pulse::def::Retval(0));
    }
}

pub fn events(tx: Sender<crate::Command>) -> Result<()> {
    let watcher = EventsWatcher::new()?;
    let ctx = watcher.context.clone();

    let prev_vol = Rc::new(RefCell::new(String::new()));
    let default_sink = Rc::new(RefCell::new(String::new()));

    let cb = move |facility, operation, index| {
        debug!("{:?} {:?} #{}", facility, operation, index);
        let prev_vol = prev_vol.clone();
        let default_sink = default_sink.clone();
        let default_sink2 = default_sink.clone();

        ctx.borrow()
            .introspect()
            .get_server_info(move |result: &ServerInfo| {
                if let Some(sink) = &result.default_sink_name {
                    *default_sink.borrow_mut() = sink.to_string();
                }
            });

        match (facility, operation) {
            (Some(Facility::Sink), Some(Operation::Changed)) => {
                let tx = tx.clone();
                ctx.borrow().introspect().get_sink_info_by_index(
                    index,
                    move |result: ListResult<&SinkInfo>| {
                        if let ListResult::Item(sink) = result {
                            debug!("{:?}", sink);

                            if sink.name.as_ref().is_some_and(|name| {
                                name.as_ref() != default_sink2.borrow().as_str()
                            }) {
                                debug!("No default");
                                return;
                            }

                            let cur_vol = format!("{}_{}", sink.volume.avg(), sink.mute);

                            if *(*prev_vol).borrow() != cur_vol {
                                let _ = tx.blocking_send(crate::Command::Volume {
                                    name: sink.proplist.get_str("device.nick"),
                                    value: sink.volume.avg().print().trim().to_string(),
                                    mute: sink.mute,
                                });

                                *prev_vol.borrow_mut() = cur_vol
                            }
                        }
                    },
                );
            }
            (Some(Facility::Source), Some(Operation::Changed)) => {
                let tx = tx.clone();
                ctx.borrow().introspect().get_source_info_by_index(
                    index,
                    move |result: ListResult<&SourceInfo>| {
                        if let ListResult::Item(source) = result {
                            debug!("{:?}", source);

                            let _ = tx.blocking_send(crate::Command::Source {
                                name: source.proplist.get_str("device.nick"),
                                ports: source
                                    .ports
                                    .iter()
                                    .filter_map(|p| p.name.as_ref().map(|n| n.to_string()))
                                    .collect(),
                            });
                        }
                    },
                );
            }
            _ => {}
        }
    };

    watcher
        .context
        .borrow_mut()
        .set_subscribe_callback(Some(Box::new(cb)));

    watcher
        .context
        .borrow_mut()
        .subscribe(InterestMaskSet::SINK | InterestMaskSet::SOURCE, |_| {});

    loop {
        watcher.mainloop.borrow_mut().iterate(true);
    }
}
