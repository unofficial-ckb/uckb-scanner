// Copyright (C) 2019 Boyu Yang
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use failure::Fail;

use uckb_jsonrpc_interfaces::types::H256;

#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "db error: {}", _0)]
    Db(postgres::Error),

    #[fail(display = "data error: {}", _0)]
    Data(String),

    #[fail(display = "data error: unknown parent block ({}, {:#x})", _0, _1)]
    UnknownParentBlock(u64, H256),
}

pub type Result<T> = ::std::result::Result<T, Error>;

impl ::std::convert::From<postgres::Error> for Error {
    fn from(error: postgres::Error) -> Self {
        Self::Db(error)
    }
}
