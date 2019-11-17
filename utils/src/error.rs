// Copyright (C) 2019 Boyu Yang
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::io;

use failure::Fail;

use uckb_jsonrpc_client::{sdk::prelude::Error as RpcError, url};

use kernel;

#[derive(Debug, Fail)]
pub(crate) enum Error {
    #[fail(display = "internal error: should be unreachable, {}", _0)]
    Unreachable(String),

    #[fail(display = "io error: {}", _0)]
    IO(io::Error),
    #[fail(display = "url error: {}", _0)]
    Url(url::ParseError),
    #[fail(display = "rpc error: {}", _0)]
    Rpc(RpcError),

    #[fail(display = "kernel error: {}", _0)]
    Kernel(kernel::error::Error),
}

pub(crate) type Result<T> = ::std::result::Result<T, Error>;

impl ::std::convert::From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::IO(error)
    }
}

impl ::std::convert::From<url::ParseError> for Error {
    fn from(error: url::ParseError) -> Self {
        Self::Url(error)
    }
}

impl ::std::convert::From<RpcError> for Error {
    fn from(error: RpcError) -> Self {
        Self::Rpc(error)
    }
}

impl ::std::convert::From<kernel::error::Error> for Error {
    fn from(error: kernel::error::Error) -> Self {
        Self::Kernel(error)
    }
}
