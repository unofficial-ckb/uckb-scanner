// Copyright (C) 2019-2020 Boyu Yang
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use uckb_jsonrpc_core::types::{core, prelude::*};

use super::Storage;
use crate::error::{Error, Result};

mod operations;

use self::operations as ops;

pub trait BaseData {
    fn initialize(&self) -> Result<Option<u64>>;
    fn destory(&self) -> Result<Vec<u64>>;
    fn insert_block(&mut self, block: &core::BlockView) -> Result<()>;
    fn remove_block(&mut self, number: u64) -> Result<()>;
    fn verify_block(&self, header: &core::HeaderView) -> Result<bool>;
}

impl BaseData for Storage {
    fn initialize(&self) -> Result<Option<u64>> {
        log::trace!("initialize the storage");
        let cli = self.client();
        self.block_on(async {
            if ops::is_first_run(cli).await? {
                ops::create_tables(cli).await?;
            }
            ops::check_current_block(cli).await
        })
    }

    fn destory(&self) -> Result<Vec<u64>> {
        log::trace!("destory the storage");
        let cli = self.client();
        self.block_on(ops::drop_tables(cli))
    }

    fn insert_block(&mut self, block: &core::BlockView) -> Result<()> {
        log::trace!("insert block {:#}", block.hash());
        if block.number() > 0 && !self.verify_block(&block.header())? {
            return Err(Error::UnknownParentBlock {
                number: block.number() - 1,
                hash: block.parent_hash().unpack(),
            });
        }
        let rt = self.runtime_clone();
        let cli = self.mut_client();
        let txn = rt.read().block_on(cli.transaction())?;
        rt.read().block_on(async {
            ops::insert_block_header(&txn, &block.header()).await?;
            let uncle_hashes = block.uncle_hashes().into_iter();
            ops::insert_block_uncles(&txn, &block.hash(), uncle_hashes).await?;
            for uncle in block.uncles().into_iter() {
                ops::insert_uncle_header(&txn, &uncle.header()).await?;
                let proposals = uncle.data().proposals().into_iter();
                ops::insert_block_proposals(&txn, &uncle.hash(), proposals).await?;
            }
            let proposals = block.data().proposals().into_iter();
            ops::insert_block_proposals(&txn, &block.hash(), proposals).await?;
            let tx_hashes = block.tx_hashes().to_owned().into_iter();
            ops::insert_block_transactions(&txn, &block.hash(), tx_hashes).await?;
            for (tx_index, tx) in block.transactions().into_iter().enumerate() {
                ops::insert_transaction(&txn, &tx, tx_index).await?;
                if tx_index != 0 {
                    let inputs = tx.data().raw().inputs().into_iter();
                    ops::consume_cells(&txn, &tx.hash(), inputs).await?;
                }
                let outputs = tx.data().raw().outputs().into_iter();
                let outputs_data = tx.data().raw().outputs_data().into_iter();
                ops::insert_cells(&txn, &tx.hash(), outputs, outputs_data).await?;
            }
            txn.commit().await.map_err(Into::<Error>::into)
        })?;
        Ok(())
    }

    fn remove_block(&mut self, number: u64) -> Result<()> {
        log::trace!("remove block {}", number);
        let rt = self.runtime_clone();
        let cli = self.mut_client();
        let block_hash_opt = rt.read().block_on(ops::query_block_hash(&cli, number))?;
        if let Some(block_hash) = block_hash_opt {
            log::trace!("remove block {:#}", block_hash);
            let txn = rt.read().block_on(cli.transaction())?;
            rt.read().block_on(async {
                let tx_hashes = ops::remove_block_transactions(&txn, &block_hash).await?;
                for tx_hash in tx_hashes.into_iter() {
                    ops::remove_transaction(&txn, &tx_hash).await?;
                    ops::restore_cells(&txn, &tx_hash).await?;
                    ops::remove_cells(&txn, &tx_hash).await?;
                }
                ops::remove_block_proposals(&txn, &block_hash).await?;
                let uncle_hashes = ops::remove_block_uncles(&txn, &block_hash).await?;
                for uncle_hash in uncle_hashes.into_iter() {
                    ops::remove_uncle_header(&txn, &uncle_hash).await?;
                    ops::remove_block_proposals(&txn, &uncle_hash).await?;
                }
                ops::remove_block_header(&txn, &block_hash).await?;
                txn.commit().await.map_err(Into::<Error>::into)
            })?;
        }
        Ok(())
    }

    fn verify_block(&self, header: &core::HeaderView) -> Result<bool> {
        log::trace!("verify block {:#}", header.hash());
        let cli = self.client();
        let sql = r#"
            SELECT 1
              FROM block_headers
             WHERE 1 = 1
               AND number = $1
               AND hash = $2
        ;"#;
        self.block_on(async {
            cli.query_opt(
                sql,
                &[
                    &(header.number() as i64 - 1),
                    &(header.parent_hash().raw_data().as_ref()),
                ],
            )
            .await
            .and_then(|row_opt| {
                row_opt
                    .map(|row| {
                        row.try_get::<_, Option<i32>>(0)
                            .map(|value| value.is_some())
                    })
                    .unwrap_or(Ok(false))
            })
        })
        .map_err(Into::into)
    }
}
