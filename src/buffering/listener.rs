use std::{pin::Pin, time::Duration};

use anyhow::Result;
use tokio::{
    select,
    sync::mpsc,
    task::JoinHandle,
    time::{Instant, Sleep, sleep_until},
};

use super::{Buffer, ListenerKey};
use crate::scanner::ScannedObject;

pub struct Listener {
    sender: mpsc::Sender<ScannedObject>,
    timeout: Duration,
    handle: JoinHandle<()>,
}

async fn listen(
    mut receiver: mpsc::Receiver<ScannedObject>,
    mut buffer: Buffer,
    timeout: Duration,
) -> Result<()> {
    let mut timer: Pin<Box<Sleep>> = Box::pin(sleep_until(Instant::now() + timeout));

    loop {
        select! {
            // Receiving an object
            maybe_object = receiver.recv() => {
                match maybe_object {
                    Some(object) => {
                        buffer.add_object(object).await?;
                        timer.as_mut().reset(Instant::now() + timeout);
                    },
                    None => {
                        log::error!(
                            "Receiver channel closed unexpectedly."
                        );
                        return buffer.flush().await;
                    }
                }
            },

            // Timer expired
            _ = &mut timer => {
                log::info!("Timeout reached. Flushing buffer.");
                buffer.flush().await?;
                timer.as_mut().reset(Instant::now() + timeout);
            }
        }
    }
}

impl Listener {
    pub fn spawn(
        listener_key: ListenerKey,
        timeout: Duration,
        channel_size: usize,
        buffer_size: usize,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(channel_size);
        Self {
            sender,
            timeout,
            handle: tokio::spawn(async move {
                if let Err(e) =
                    listen(receiver, Buffer::new(listener_key, buffer_size), timeout).await
                {
                    log::error!("Listener encountered an error: {:?}", e);
                }
            }),
        }
    }

    /// Returns a clone of the sender, safe to use across threads/tasks.
    pub fn get_new_sender(&self) -> mpsc::Sender<ScannedObject> {
        self.sender.clone()
    }
}
