// Copyright (C) 2019-2020 Boyu Yang
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::{
    sync::{atomic, Arc},
    thread,
    time::Duration,
};

use jsonrpc_server_utils::tokio::runtime as runtime01;
use kernel::{error::Error as KernelError, traits::BaseData as _, Storage};
use parking_lot::RwLock;
use tokio::runtime;
use uckb_jsonrpc_client::Client;

use crate::{config::SyncArgs, error::Result};

pub(crate) fn execute(args: SyncArgs) -> Result<()> {
    let rt = initialize_runtime().map(sync_lock)?;
    let rt01 = initialize_runtime01().map(sync_lock)?;
    let mut storage = Storage::connect(Arc::clone(&rt), args.storage_uri())?;
    let client = {
        let mut client = Client::new(Arc::clone(&rt), Arc::clone(&rt01));
        client
            .enable_http(args.jsonrpc_url())?
            .enable_tcp(args.subscribe_socket())?;
        client
    };
    let mut next = storage.initialize()?.map(|n| n + 1).unwrap_or(0);
    log::info!("current storage has base data before height {}", next);
    loop {
        let tip = client.get_tip_block_number()?;
        log::info!("current tip number is {}", tip);
        let mut rollback_to = None;
        for i in next..=tip {
            log::info!("synchronize block {} ...", i);
            if let Some(block) = client.get_block_by_number(i, None)? {
                let result = storage.insert_block(&block);
                if let Err(KernelError::UnknownParentBlock { number, hash }) = result {
                    log::warn!("rollback unknown parent block ({}, {:#x})", number, hash);
                    storage.remove_block(number)?;
                    rollback_to = Some(number);
                    break;
                } else {
                    result?;
                }
            } else {
                break;
            }
        }
        next = if let Some(rollback_to) = rollback_to {
            rollback_to
        } else {
            let wait_secs = Duration::from_secs(2);
            thread::sleep(wait_secs);
            tip + 1
        };
    }
}

pub(crate) fn initialize_runtime() -> Result<runtime::Runtime> {
    runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .max_threads(32)
        .enable_time()
        .enable_io()
        .thread_name_fn(|| {
            static ATOMIC_ID: atomic::AtomicUsize = atomic::AtomicUsize::new(0);
            let id = ATOMIC_ID.fetch_add(1, atomic::Ordering::SeqCst);
            format!("runtime-{}", id)
        })
        .build()
        .map_err(Into::into)
}

pub(crate) fn initialize_runtime01() -> Result<runtime01::Runtime> {
    runtime01::Builder::new()
        .blocking_threads(16)
        .core_threads(2)
        .name_prefix("runtime01-")
        .build()
        .map_err(Into::into)
}

pub(crate) fn sync_lock<T>(inner: T) -> Arc<RwLock<T>> {
    Arc::new(RwLock::new(inner))
}
