use super::Buffer;
use crate::scanner::ScannedObject;
use anyhow::Result;
use std::pin::Pin;
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Instant, Sleep, sleep_until};

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

pub struct Listener {
    sender: mpsc::Sender<ScannedObject>,
    timeout: Duration,
    handle: JoinHandle<()>,
}

impl Listener {
    pub fn spawn(buffer: Buffer, timeout: Duration, channel_size: usize) -> Self {
        let (sender, receiver) = mpsc::channel(channel_size);
        Self {
            sender,
            timeout,
            handle: tokio::spawn(async move {
                if let Err(e) = listen(receiver, buffer, timeout).await {
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
