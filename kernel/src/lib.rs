// Copyright (C) 2019-2020 Boyu Yang
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::sync::Arc;

use tokio::runtime::Runtime as RawRuntime;

pub use tokio_postgres as postgres;

pub mod error;

mod storage;
mod utilities;

pub use storage::{traits, Storage};

pub(crate) type Runtime = Arc<RawRuntime>;
