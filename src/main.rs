use std::{cell::RefCell, sync::Arc};

use pulse::{
    context::{subscribe::{InterestMaskSet, Facility}, Context, FlagSet, State},
    mainloop::standard::Mainloop,
    proplist::Proplist, callbacks::ListResult
};
use zbus::blocking::Connection;

use anyhow::Result;
use log::debug;

const PA_NAME: &str = "pa-events-watcher";

pub struct EventsWatcher {
    mainloop: Mainloop,
    context: Arc<RefCell<Context>>,
    dbus: Arc<RefCell<Connection>>
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

        context.connect(None, FlagSet::NOFLAGS, None)
            .expect("Failed to connect to default server");

        loop {
            mainloop.iterate(true);
            match context.get_state() {
                State::Ready => { debug!("ready"); break; },
                _ => { debug!("wait") }
            }
        }

        let dbus = Connection::session().expect("Failed to connect to dbus");

        Ok(EventsWatcher {
            mainloop,
            context: Arc::new(RefCell::new(context)),
            dbus: Arc::new(RefCell::new(dbus))
        })
    }
}

fn main() {
    let interest = InterestMaskSet::SINK | InterestMaskSet::SOURCE;
    // let interest = InterestMaskSet::ALL;

    let mut ew = EventsWatcher::new().expect("Failed to create mainloop or context");

    let ctx = ew.context.clone();

    let cb = Box::new(move |facility, operation, index| {
        println!("{:?} {:?} #{}", facility, operation, index);
        let dbus = ew.dbus.clone();

        let dbus_call = move |code: &str| {
            let reply = dbus.borrow().call_method(
                Some("org.awesomewm.awful"), "/",
                Some("org.awesomewm.awful.Remote"), "Eval",
                &code
            );

            match reply {
                Ok(message) => { debug!("{:?}", message) },
                Err(_) => {},
            }
        };

        // ctx.borrow().introspect().get_server_info(|result| {
        //     println!("{:?}", result);
        // });

        match facility {
            Some(Facility::Sink) => {
                ctx.borrow().introspect().get_sink_info_by_index(index, move |result| {
                    match result {
                        ListResult::Item(sink) => {
                            let code = format!(
                                "awesome.emit_signal('volume::change', '{}', {})",
                                sink.volume.avg().print(), sink.mute
                            );

                            debug!("{}", &code);

                            dbus_call(&code);
                        }
                        _ => {}
                    }
                });
            },
            Some(Facility::Source) => {
                ctx.borrow().introspect().get_source_info_by_index(index, move |result| {
                    match result {
                        ListResult::Item(source) => {
                            // println!("{:?}", source);

                            for ele in &source.ports {
                                println!("{:?}", ele);
                            }
                        }
                        _ => {}
                    }
                });
            }
            _ => {}
        }
    });

    ew.context.borrow_mut().set_subscribe_callback(Some(cb));
    ew.context.borrow_mut().subscribe(interest, |_| {});

    loop {
        ew.mainloop.iterate(true);
    }
}
