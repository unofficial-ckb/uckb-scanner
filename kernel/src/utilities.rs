// Copyright (C) 2019-2020 Boyu Yang
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use property::Property;

#[derive(Property)]
#[property(get(public), set(disable), mut(disable))]
pub(crate) struct Dao {
    c: u64,
    ar: u64,
    s: u64,
    u: u64,
}

impl Dao {
    pub(crate) fn from_slice(slice: &[u8]) -> Self {
        let mut tmp = [0u8; 8];
        tmp.copy_from_slice(&slice[0..8]);
        let c = u64::from_le_bytes(tmp);
        tmp.copy_from_slice(&slice[8..16]);
        let ar = u64::from_le_bytes(tmp);
        tmp.copy_from_slice(&slice[16..24]);
        let s = u64::from_le_bytes(tmp);
        tmp.copy_from_slice(&slice[24..32]);
        let u = u64::from_le_bytes(tmp);
        Self { c, ar, s, u }
    }
}
