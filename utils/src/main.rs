// Copyright (C) 2019-2020 Boyu Yang
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

mod config;
mod error;
mod subcmd;

fn main() -> anyhow::Result<()> {
    env_logger::init();

    log::info!("starting ...");

    let config = config::build_commandline()?;
    match config {
        config::AppConfig::Sync(args) => subcmd::sync::execute(args),
    }?;

    log::info!("done.");

    Ok(())
}
