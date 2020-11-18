// Copyright (C) 2019-2020 Boyu Yang
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use uckb_jsonrpc_core::types::{packed, prelude::*};

use crate::{
    error::{Error, Result},
    postgres as pg,
};

pub(super) fn hash_from_value(hash_vec: Vec<u8>) -> Result<packed::Byte32> {
    if hash_vec.len() == 32 {
        let mut hash_array = [0u8; 32];
        hash_array.copy_from_slice(&hash_vec[..]);
        Ok(hash_array.pack())
    } else {
        Err(Error::Data(format!(
            "incorrect block hash (length: {})",
            hash_vec.len()
        )))
    }
}

pub(super) async fn drop_table(cli: &pg::Client, table: &str) -> Result<u64> {
    let sql = format!("DROP TABLE IF EXISTS {};", table);
    cli.execute(sql.as_str(), &[]).await.map_err(Into::into)
}
