use log::debug;
use std::{
    borrow::{Borrow, Cow},
    cell::RefCell,
    collections::{HashMap, HashSet},
    sync::Arc,
};
use anyhow::Result;

use pulse::{
    callbacks::ListResult,
    context::{
        introspect::{CardInfo, SinkInfo, SourceInfo, SourcePortInfo},
        subscribe::{Facility, InterestMaskSet},
        Context, FlagSet, State,
    },
    def::PortAvailable,
    mainloop::standard::Mainloop,
    proplist::Proplist,
};
use zbus::blocking::Connection;

const PA_NAME: &str = "pa-events-watcher";

pub struct EventsWatcher {
    mainloop: Mainloop,
    context: Arc<RefCell<Context>>,
    dbus: Arc<RefCell<Connection>>,
}

impl EventsWatcher {
    fn new() -> Result<EventsWatcher> {
        let mut mainloop = Mainloop::new().expect("Failed to create mainloop");
        let mut proplist = Proplist::new().expect("Failed to create proplist");

        proplist
            .set_str(pulse::proplist::properties::APPLICATION_NAME, PA_NAME)
            .expect("Failed to set APPLICATION_NAME");

        let mut context = Context::new_with_proplist(&mainloop, PA_NAME, &proplist)
            .expect("Failed to create new context");

        context
            .connect(None, FlagSet::NOFLAGS, None)
            .expect("Failed to connect to default server");

        loop {
            mainloop.iterate(true);
            match context.get_state() {
                State::Ready => {
                    debug!("ready");
                    break;
                }
                _ => {
                    debug!("wait")
                }
            }
        }

        Ok(EventsWatcher {
            mainloop,
            context: Arc::new(RefCell::new(context)),
            dbus: Arc::new(RefCell::new(
                Connection::session().expect("Failed to connect to dbus"),
            )),
        })
    }
}

fn events() {
    let interest = InterestMaskSet::SINK | InterestMaskSet::SOURCE;
    // let interest = InterestMaskSet::ALL;

    let mut watcher = EventsWatcher::new().expect("Failed to create mainloop or context");

    let dbus = watcher.dbus.clone();

    let dbus_call = move |code: &str| {
        let reply = (*dbus).borrow().call_method(
            Some("org.awesomewm.awful"),
            "/",
            Some("org.awesomewm.awful.Remote"),
            "Eval",
            &code,
        );

        match reply {
            Ok(message) => {
                debug!("{:?}", message)
            }
            Err(_) => {}
        }
    };

    let dbus_call2 = dbus_call.clone();

    let prev_vol = Arc::new(RefCell::new(String::new()));

    let sink_cb = move |result: ListResult<&SinkInfo>| match result {
        ListResult::Item(sink) => {
            let cur_vol = format!("{}_{}", sink.volume.avg(), sink.mute);

            if *(*prev_vol).borrow() != cur_vol {
                let code = format!(
                    "awesome.emit_signal('volume::change', '{}', {})",
                    sink.volume.avg().print().trim(),
                    sink.mute
                );

                debug!("{}", &code);

                dbus_call(&code);
                *prev_vol.borrow_mut() = cur_vol;
            } else {
                debug!("same vol");
            }
        }
        _ => {}
    };

    let card_cb = move |result: ListResult<&CardInfo>| {
        match result {
            ListResult::Item(card) => {
                // println!("source state: {:?}", source.state);
                // println!("{:?}", card.profiles);
                for profile in &card.profiles {
                    if profile.available {
                        println!(
                            "{:?} {:?}",
                            profile.name.as_ref().unwrap(),
                            profile.description.as_ref().unwrap()
                        );
                    }
                }
                println!("{:?}", card.active_profile);
            }
            _ => {}
        }
    };

    let prev_ports = Arc::new(RefCell::new(HashSet::new()));

    let source_cb = move |result: ListResult<&SourceInfo>| {
        match result {
            ListResult::Item(source) => {
                // println!("{:?}", source);

                // for ele in &source.ports {
                //     println!("{:?} {:?} {:?}", ele.name, ele.description, ele.available);
                // }

                let mut ports: HashSet<String> = HashSet::new();
                for port in &source.ports {
                    if port.available != PortAvailable::No {
                        ports.insert(port.name.clone().unwrap().to_string());
                    }
                }

                if *(*prev_ports).borrow() != ports {
                    if ports.len() > 1 {
                        let code = format!(
                            "awesome.emit_signal('headset::connected', '{}')",
                            ports
                                .clone()
                                .into_iter()
                                .collect::<Vec<String>>()
                                .join("', '")
                        );

                        println!("{}", &code);

                        dbus_call2(&code);
                    }

                    println!("{:?}", ports);

                    *prev_ports.borrow_mut() = ports;
                }
            }
            _ => {}
        }
    };

    let ctx = watcher.context.clone();
    let cb = move |facility, operation, index| {
        println!("{:?} {:?} #{}", facility, operation, index);

        // ctx.borrow().introspect().get_server_info(|result| {
        //     println!("{:?}", result);
        // });

        match facility {
            Some(Facility::Sink) => {
                (*ctx)
                    .borrow()
                    .introspect()
                    .get_sink_info_by_index(index, sink_cb.clone());
            }
            Some(Facility::Source) => {
                (*ctx)
                    .borrow()
                    .introspect()
                    .get_source_info_by_index(index, source_cb.clone());
            }
            // Some(Facility::Card) => {
            //     (*ctx).borrow().introspect().get_card_info_by_index(index, card_cb.clone());
            // },
            _ => {}
        }
    };

    watcher
        .context
        .borrow_mut()
        .set_subscribe_callback(Some(Box::new(cb)));
    watcher.context.borrow_mut().subscribe(interest, |_| {});

    loop {
        watcher.mainloop.iterate(true);
    }
}

