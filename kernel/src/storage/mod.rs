// Copyright (C) 2019-2020 Boyu Yang
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::{future::Future, sync::Arc};

use property::Property;

use crate::{error::Result, postgres as pg, Runtime};

mod base_data;
mod operations;
pub mod traits;

#[derive(Property)]
#[property(get(public), set(disable), mut(crate))]
pub struct Storage {
    client: pg::Client,
    #[property(get(disable))]
    runtime: Runtime,
}

impl Storage {
    pub fn connect(rt: Runtime, uri: &str) -> Result<Self> {
        let (client, connection) = rt.read().block_on(pg::connect(uri, pg::NoTls))?;
        rt.read().spawn(async {
            if let Err(err) = connection.await {
                log::error!("connection error: {}", err);
            }
        });
        Ok(Self {
            client,
            runtime: rt,
        })
    }

    pub fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        log::trace!("block on a future");
        self.runtime().read().block_on(future)
    }

    pub fn runtime(&self) -> Runtime {
        Arc::clone(&self.runtime)
    }
}
