// Copyright (C) 2019 Boyu Yang
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::convert::TryFrom;

use property::Property;

use uckb_jsonrpc_client::url;

use crate::error::{Error, Result};

#[derive(Property)]
pub(crate) struct Arguments {
    url: url::Url,
    db_uri: String,
}

pub(crate) fn build_commandline() -> Result<Arguments> {
    let yaml = clap::load_yaml!("cli.yaml");
    let matches = clap::App::from_yaml(yaml).get_matches();
    Arguments::try_from(&matches)
}

impl<'a> TryFrom<&'a clap::ArgMatches<'a>> for Arguments {
    type Error = Error;
    fn try_from(matches: &'a clap::ArgMatches) -> Result<Self> {
        let url = matches
            .value_of("url")
            .map(|url_str| url::Url::parse(url_str))
            .transpose()?
            .ok_or_else(|| Error::Unreachable("no argument 'url'".to_owned()))?;
        let db_uri = matches
            .value_of("db-uri")
            .map(ToOwned::to_owned)
            .ok_or_else(|| Error::Unreachable("no argument 'db-uri'".to_owned()))?;
        Ok(Self { url, db_uri })
    }
}
