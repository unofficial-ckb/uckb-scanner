// Copyright (C) 2019-2020 Boyu Yang
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::io;

use thiserror::Error;

use uckb_jsonrpc_client::{error::Error as RpcError, url};

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error("internal error: should be unreachable, {0}")]
    Unreachable(String),

    #[error("io error: {0}")]
    IO(#[from] io::Error),
    #[error("url error: {0}")]
    Url(#[from] url::ParseError),
    #[error("rpc error: {0}")]
    Rpc(#[from] RpcError),

    #[error("kernel error: {0}")]
    Kernel(#[from] kernel::error::Error),
}

pub(crate) type Result<T> = ::std::result::Result<T, Error>;
