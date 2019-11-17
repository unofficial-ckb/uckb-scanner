// Copyright (C) 2019 Boyu Yang
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::{process, thread, time};

mod arguments;
mod error;

use kernel::Storage;

use uckb_jsonrpc_client::client::CkbSyncClient;

fn main() {
    pretty_env_logger::init_timed();

    log::info!("Begin to run ...");

    if let Err(error) = execute() {
        eprintln!("fatal: {}", error);
        process::exit(1);
    }

    log::info!("Exit.");
}

fn execute() -> error::Result<()> {
    let args = arguments::build_commandline()?;

    let client = CkbSyncClient::new(args.url().to_owned());

    let storage = Storage::connect(args.db_uri())?;
    let mut next = storage.initialize()?.unwrap_or(0);
    log::info!("    current storage has chain data before height {}", next);

    loop {
        let tip = client.tip_number()?;
        for i in next..=tip {
            log::info!("        synchronize block {} ...", i);
            let block = client.block_by_number(i)?;
            storage.insert_block(&block)?;
        }
        next = tip + 1;
        let wait_secs = time::Duration::from_secs(2);
        thread::sleep(wait_secs);
    }
}
