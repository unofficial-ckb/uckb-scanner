// Copyright (C) 2019-2020 Boyu Yang
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use thiserror::Error;
use uckb_jsonrpc_core::types::fixed::H256;

use crate::postgres as pg;

#[derive(Debug, Error)]
pub enum Error {
    #[error("inner db error: {0}")]
    InnerDB(#[from] pg::Error),

    #[error("data error: {0}")]
    Data(String),

    #[error("data error: unknown parent block ({number}, {hash:#x})")]
    UnknownParentBlock { number: u64, hash: H256 },
}

pub type Result<T> = ::std::result::Result<T, Error>;
