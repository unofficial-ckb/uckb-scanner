// Copyright (C) 2019-2020 Boyu Yang
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::{convert::TryFrom, net::SocketAddr};

use property::Property;

use uckb_jsonrpc_client::url;

use crate::error::{Error, Result};

pub(crate) enum AppConfig {
    Sync(SyncArgs),
}

#[derive(Property)]
pub(crate) struct SyncArgs {
    jsonrpc_url: url::Url,
    subscribe_socket: SocketAddr,
    storage_uri: String,
}

pub(crate) fn build_commandline() -> Result<AppConfig> {
    let yaml = clap::load_yaml!("cli.yaml");
    let matches = clap::App::from_yaml(yaml)
        .version(clap::crate_version!())
        .author(clap::crate_authors!("\n"))
        .get_matches();
    AppConfig::try_from(&matches)
}

impl<'a> TryFrom<&'a clap::ArgMatches<'a>> for AppConfig {
    type Error = Error;
    fn try_from(matches: &'a clap::ArgMatches) -> Result<Self> {
        match matches.subcommand() {
            ("sync", Some(matches)) => SyncArgs::try_from(matches).map(AppConfig::Sync),
            _ => unreachable!(),
        }
    }
}

impl<'a> TryFrom<&'a clap::ArgMatches<'a>> for SyncArgs {
    type Error = Error;
    fn try_from(matches: &'a clap::ArgMatches) -> Result<Self> {
        let jsonrpc_url = matches
            .value_of("jsonrpc-url")
            .map(|url_str| url::Url::parse(url_str))
            .transpose()?
            .ok_or_else(|| Error::Unreachable("no argument 'jsonrpc-url'".to_owned()))?;
        let subscribe_socket = matches
            .value_of("subscribe-socket")
            .map(|addr_str| addr_str.parse().unwrap())
            .ok_or_else(|| Error::Unreachable("no argument 'subscribe-socket'".to_owned()))?;
        let storage_uri = matches
            .value_of("storage-uri")
            .map(ToOwned::to_owned)
            .ok_or_else(|| Error::Unreachable("no argument 'storage-uri'".to_owned()))?;
        Ok(Self {
            jsonrpc_url,
            subscribe_socket,
            storage_uri,
        })
    }
}
