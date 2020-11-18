// Copyright (C) 2019-2020 Boyu Yang
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use futures::future::try_join_all;
use uckb_jsonrpc_core::types::{core, packed, prelude::*};

use super::super::operations as ops;
use crate::{error::Result, postgres as pg, utilities::Dao};

pub(super) async fn is_first_run(cli: &pg::Client) -> Result<bool> {
    log::trace!("check if is the first run");
    cli.query("SELECT 1 FROM block_headers;", &[])
        .await
        .map(|_| false)
        .or_else(|err| {
            let undefined = err
                .code()
                .map(|s| *s == pg::error::SqlState::UNDEFINED_TABLE)
                .unwrap_or(false);
            if undefined {
                Ok(true)
            } else {
                Err(err)
            }
        })
        .map_err(Into::into)
}

pub(super) async fn create_tables(cli: &pg::Client) -> Result<Vec<u64>> {
    log::trace!("create all tables");
    let mut sqls = Vec::new();
    {
        let sql = r#"
            CREATE TABLE IF NOT EXISTS block_headers (
                hash                BYTEA       NOT NULL PRIMARY KEY,
                version             INTEGER     NOT NULL,
                compact_target      BIGINT      NOT NULL,
                timestamp           BIGINT      NOT NULL,
                number              BIGINT      NOT NULL UNIQUE,
                epoch_number        INTEGER     NOT NULL,
                epoch_index         INTEGER     NOT NULL,
                epoch_length        INTEGER     NOT NULL,
                parent_hash         BYTEA       NOT NULL,
                transactions_root   BYTEA       NOT NULL,
                proposals_hash      BYTEA       NOT NULL,
                uncles_hash         BYTEA       NOT NULL,
                dao_c               BIGINT      NOT NULL,
                dao_ar              BIGINT      NOT NULL,
                dao_s               BIGINT      NOT NULL,
                dao_u               BIGINT      NOT NULL,
                nonce               BYTEA       NOT NULL
            );"#;
        sqls.push(sql);
    }
    {
        let sql = r#"
            CREATE TABLE IF NOT EXISTS block_uncles (
                block_hash          BYTEA       NOT NULL,
                uncle_hash          BYTEA       NOT NULL,
                index               INTEGER     NOT NULL,
                PRIMARY KEY (block_hash, uncle_hash)
            );"#;
        sqls.push(sql);
    }
    {
        let sql = r#"
            CREATE TABLE IF NOT EXISTS uncle_headers (
                hash                BYTEA       NOT NULL PRIMARY KEY,
                version             INTEGER     NOT NULL,
                compact_target      BIGINT      NOT NULL,
                timestamp           BIGINT      NOT NULL,
                number              BIGINT      NOT NULL,
                epoch_number        INTEGER     NOT NULL,
                epoch_index         INTEGER     NOT NULL,
                epoch_length        INTEGER     NOT NULL,
                parent_hash         BYTEA       NOT NULL,
                transactions_root   BYTEA       NOT NULL,
                proposals_hash      BYTEA       NOT NULL,
                uncles_hash         BYTEA       NOT NULL,
                dao_c               BIGINT      NOT NULL,
                dao_ar              BIGINT      NOT NULL,
                dao_s               BIGINT      NOT NULL,
                dao_u               BIGINT      NOT NULL,
                nonce               BYTEA       NOT NULL
            );"#;
        sqls.push(sql);
    }
    {
        let sql = r#"
            CREATE TABLE IF NOT EXISTS block_proposals (
                block_hash          BYTEA       NOT NULL,
                short_id            BYTEA       NOT NULL,
                index               INTEGER     NOT NULL,
                PRIMARY KEY (block_hash, short_id)
            );"#;
        sqls.push(sql);
    }
    {
        let sql = r#"
            CREATE TABLE IF NOT EXISTS block_transactions (
                block_hash          BYTEA       NOT NULL,
                tx_hash             BYTEA       NOT NULL,
                index               INTEGER     NOT NULL,
                PRIMARY KEY (block_hash, tx_hash)
            );"#;
        sqls.push(sql);
    }
    {
        let sql = r#"
            CREATE TABLE IF NOT EXISTS transactions (
                hash                BYTEA       NOT NULL PRIMARY KEY,
                version             INTEGER     NOT NULL
            );"#;
        sqls.push(sql);
    }
    {
        let sql = r#"
            CREATE TABLE IF NOT EXISTS tx_cell_deps (
                ref_tx_hash         BYTEA       NOT NULL,
                ref_index           INTEGER     NOT NULL,
                ref_dep_index       INTEGER     NOT NULL,
                tx_hash             BYTEA       NOT NULL,
                index               INTEGER     NOT NULL,
                dep_type            SMALLINT    NOT NULL,
                PRIMARY KEY (ref_tx_hash, ref_index, ref_dep_index)
            );"#;
        sqls.push(sql);
    }
    {
        let sql = r#"
            CREATE TABLE IF NOT EXISTS tx_header_deps (
                ref_tx_hash         BYTEA       NOT NULL,
                ref_index           INTEGER     NOT NULL,
                ref_dep_index       INTEGER     NOT NULL,
                block_hash          BYTEA       NOT NULL,
                PRIMARY KEY (ref_tx_hash, ref_index, ref_dep_index)
            );"#;
        sqls.push(sql);
    }
    {
        let sql = r#"
            CREATE TABLE IF NOT EXISTS tx_witnesses (
                ref_tx_hash         BYTEA       NOT NULL,
                ref_index           INTEGER     NOT NULL,
                ref_dep_index       INTEGER     NOT NULL,
                witness             BYTEA       NOT NULL,
                PRIMARY KEY (ref_tx_hash, ref_index, ref_dep_index)
            );"#;
        sqls.push(sql);
    }
    {
        let sql = r#"
            CREATE TABLE IF NOT EXISTS cells (
                tx_hash             BYTEA       NOT NULL,
                index               INTEGER     NOT NULL,
                capacity            BIGINT      NOT NULL,
                lock_hash           BYTEA       NOT NULL,
                type_hash           BYTEA,
                data_hash           BYTEA       NOT NULL,
                consumed_tx_hash    BYTEA,
                consumed_index      INTEGER,
                consumed_since      BYTEA,
                PRIMARY KEY (tx_hash, index)
            );"#;
        sqls.push(sql);
    }
    {
        let sql = r#"
            CREATE TABLE IF NOT EXISTS cells_data (
                hash                BYTEA       NOT NULL PRIMARY KEY,
                data                BYTEA       NOT NULL
            );"#;
        sqls.push(sql);
    }
    {
        let sql = r#"
            CREATE TABLE IF NOT EXISTS scripts (
                hash                BYTEA       NOT NULL PRIMARY KEY,
                code_hash           BYTEA       NOT NULL,
                hash_type           SMALLINT    NOT NULL,
                args                BYTEA       NOT NULL
            );"#;
        sqls.push(sql);
    }
    let futures = sqls
        .into_iter()
        .map(|sql| cli.execute(sql, &[]))
        .collect::<Vec<_>>();
    try_join_all(futures).await.map_err(Into::into)
}

pub(super) async fn drop_tables(cli: &pg::Client) -> Result<Vec<u64>> {
    log::trace!("drop all tables");
    let tables = &[
        "block_headers",
        "block_uncles",
        "uncle_headers",
        "block_proposals",
        "block_transactions",
        "transactions",
        "tx_cell_deps",
        "tx_header_deps",
        "tx_witnesses",
        "cells",
        "cells_data",
        "scripts",
    ];
    let futures = tables
        .iter()
        .map(|name| ops::drop_table(cli, name))
        .collect::<Vec<_>>();
    try_join_all(futures).await
}

pub(super) async fn check_current_block(cli: &pg::Client) -> Result<Option<u64>> {
    log::trace!("check the number of current block");
    cli.query_one("SELECT MAX(number) FROM block_headers;", &[])
        .await
        .and_then(|row| {
            row.try_get::<_, Option<i64>>(0)
                .map(|num_opt| num_opt.map(|num| num as u64))
        })
        .map_err(Into::into)
}

pub(super) async fn query_block_hash(
    cli: &pg::Client,
    number: u64,
) -> Result<Option<packed::Byte32>> {
    log::trace!("query block by number {}", number);
    let sql = r#"
        SELECT hash
          FROM block_headers
         WHERE 1 = 1
           AND number = $1
    ;"#;
    cli.query_opt(sql, &[&(number as i64)])
        .await
        .map_err(Into::into)
        .and_then(|row_opt| {
            row_opt
                .and_then(|row| {
                    row.try_get::<_, Option<Vec<u8>>>(0)
                        .map_err(Into::into)
                        .transpose()
                        .map(|value_opt| value_opt.and_then(ops::hash_from_value))
                })
                .transpose()
        })
}

async fn insert_header(
    txn: &pg::Transaction<'_>,
    table_name: &str,
    header: &core::HeaderView,
) -> Result<u64> {
    let sql = format!(
        r#"
        INSERT INTO {} (
            hash, version, compact_target, timestamp,
            number, epoch_number, epoch_index, epoch_length,
            parent_hash, transactions_root, proposals_hash, uncles_hash,
            dao_c, dao_ar, dao_s, dao_u, nonce
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17
        )
        ON CONFLICT DO NOTHING
    ;"#,
        table_name
    );
    let dao = Dao::from_slice(header.dao().raw_data().as_ref());
    txn.execute(
        sql.as_str(),
        &[
            &header.hash().raw_data().as_ref(),
            &(header.version() as i32),
            &(header.compact_target() as i64),
            &(header.timestamp() as i64),
            &(header.number() as i64),
            &(header.epoch().number() as i32),
            &(header.epoch().index() as i32),
            &(header.epoch().length() as i32),
            &header.parent_hash().raw_data().as_ref(),
            &header.transactions_root().raw_data().as_ref(),
            &header.proposals_hash().raw_data().as_ref(),
            &header.uncles_hash().raw_data().as_ref(),
            &(dao.c() as i64),
            &(dao.ar() as i64),
            &(dao.s() as i64),
            &(dao.u() as i64),
            &header.data().nonce().raw_data().as_ref(),
        ],
    )
    .await
    .map_err(Into::into)
}

pub(super) async fn insert_block_header(
    txn: &pg::Transaction<'_>,
    header: &core::HeaderView,
) -> Result<u64> {
    log::trace!("insert block header {:#}", header.hash());
    insert_header(txn, "block_headers", header).await
}

pub(super) async fn remove_block_header(
    txn: &pg::Transaction<'_>,
    block_hash: &packed::Byte32,
) -> Result<u64> {
    log::trace!("remove block header {:#}", block_hash);
    let sql = r#"DELETE FROM block_headers WHERE hash = $1;"#;
    txn.execute(sql, &[&block_hash.raw_data().as_ref()])
        .await
        .map_err(Into::into)
}

pub(super) async fn insert_block_uncles(
    txn: &pg::Transaction<'_>,
    block_hash: &packed::Byte32,
    uncle_hashes: impl Iterator<Item = packed::Byte32>,
) -> Result<()> {
    log::trace!("insert uncles for block {:#}", block_hash);
    let sql = r#"
        INSERT INTO block_uncles (
            block_hash, uncle_hash, index
        ) VALUES (
            $1, $2, $3
        )
        ON CONFLICT DO NOTHING
    ;"#;
    let stmt = txn.prepare(sql).await?;
    for (index, uncle_hash) in uncle_hashes.enumerate() {
        txn.execute(
            &stmt,
            &[
                &block_hash.raw_data().as_ref(),
                &uncle_hash.raw_data().as_ref(),
                &(index as i32),
            ],
        )
        .await?;
    }
    Ok(())
}

pub(super) async fn remove_block_uncles(
    txn: &pg::Transaction<'_>,
    block_hash: &packed::Byte32,
) -> Result<Vec<packed::Byte32>> {
    log::trace!("remove uncles for block {:#}", block_hash);
    let sql = r#"
        DELETE FROM block_uncles
         WHERE block_hash = $1
     RETURNING uncle_hash
    ;"#;
    txn.query(sql, &[&block_hash.raw_data().as_ref()])
        .await
        .map_err(Into::into)
        .and_then(|ref rows| {
            rows.iter()
                .map(|ref row| {
                    row.try_get::<_, Vec<u8>>(0)
                        .map_err(Into::into)
                        .and_then(ops::hash_from_value)
                })
                .collect::<Result<Vec<packed::Byte32>>>()
        })
}

pub(super) async fn insert_uncle_header(
    txn: &pg::Transaction<'_>,
    header: &core::HeaderView,
) -> Result<u64> {
    log::trace!("insert uncle header {:#}", header.hash());
    insert_header(txn, "uncle_headers", header).await
}

pub(super) async fn remove_uncle_header(
    txn: &pg::Transaction<'_>,
    uncle_hash: &packed::Byte32,
) -> Result<u64> {
    log::trace!("remove uncle header {:#}", uncle_hash);
    let sql = r#"
        DELETE FROM uncle_headers uh
              WHERE 1 = 1
                AND hash = $1
                AND NOT EXISTS (
                    SELECT 1
                      FROM block_uncles bu
                     WHERE bu.uncle_hash = uh.hash)
    ;"#;
    txn.execute(sql, &[&uncle_hash.raw_data().as_ref()])
        .await
        .map_err(Into::into)
}

pub(super) async fn insert_block_proposals(
    txn: &pg::Transaction<'_>,
    block_hash: &packed::Byte32,
    proposals: impl Iterator<Item = packed::ProposalShortId>,
) -> Result<()> {
    log::trace!("insert proposals for block {:#}", block_hash);
    let sql = r#"
        INSERT INTO block_proposals (
            block_hash, short_id, index
        ) VALUES (
            $1, $2, $3
        )
        ON CONFLICT DO NOTHING
    ;"#;
    let stmt = txn.prepare(sql).await?;
    for (index, proposal) in proposals.enumerate() {
        txn.execute(
            &stmt,
            &[
                &block_hash.raw_data().as_ref(),
                &proposal.raw_data().as_ref(),
                &(index as i32),
            ],
        )
        .await?;
    }
    Ok(())
}

pub(super) async fn remove_block_proposals(
    txn: &pg::Transaction<'_>,
    block_hash: &packed::Byte32,
) -> Result<u64> {
    log::trace!("remove proposals for block {:#}", block_hash);
    let sql = r#"
        DELETE FROM block_proposals bp
              WHERE 1 = 1
                AND block_hash = $1
                AND NOT EXISTS (
                    SELECT 1
                      FROM block_headers bh
                     WHERE bh.hash = bp.block_hash)
                AND NOT EXISTS (
                    SELECT 1
                      FROM block_uncles bu
                     WHERE bu.uncle_hash = bp.block_hash)
    ;"#;
    txn.execute(sql, &[&block_hash.raw_data().as_ref()])
        .await
        .map_err(Into::into)
}

pub(super) async fn insert_block_transactions(
    txn: &pg::Transaction<'_>,
    block_hash: &packed::Byte32,
    tx_hashes: impl Iterator<Item = packed::Byte32>,
) -> Result<()> {
    log::trace!("insert transactions for block {:#}", block_hash);
    let sql = r#"
        INSERT INTO block_transactions (
            block_hash, tx_hash, index
        ) VALUES (
            $1, $2, $3
        )
        ON CONFLICT DO NOTHING
    ;"#;
    let stmt = txn.prepare(sql).await?;
    for (index, tx_hash) in tx_hashes.enumerate() {
        txn.execute(
            &stmt,
            &[
                &block_hash.raw_data().as_ref(),
                &tx_hash.raw_data().as_ref(),
                &(index as i32),
            ],
        )
        .await?;
    }
    Ok(())
}

pub(super) async fn remove_block_transactions(
    txn: &pg::Transaction<'_>,
    block_hash: &packed::Byte32,
) -> Result<Vec<packed::Byte32>> {
    log::trace!("remove transactions for block {:#}", block_hash);
    let sql = r#"
        DELETE FROM block_transactions
         WHERE block_hash = $1
     RETURNING tx_hash
    ;"#;
    txn.query(sql, &[&block_hash.raw_data().as_ref()])
        .await
        .map_err(Into::into)
        .and_then(|ref rows| {
            rows.iter()
                .map(|ref row| {
                    row.try_get::<_, Vec<u8>>(0)
                        .map_err(Into::into)
                        .and_then(ops::hash_from_value)
                })
                .collect::<Result<Vec<packed::Byte32>>>()
        })
}

pub(super) async fn insert_transaction(
    txn: &pg::Transaction<'_>,
    tx: &core::TransactionView,
    ref_index: usize,
) -> Result<u64> {
    log::trace!("insert transaction {:#}", tx.hash());
    {
        let sql = r#"
            INSERT INTO tx_cell_deps (
                ref_tx_hash, ref_index, ref_dep_index, tx_hash, index, dep_type
            ) VALUES (
                $1, $2, $3, $4, $5, $6
            )
            ON CONFLICT DO NOTHING
        ;"#;
        let stmt = txn.prepare(sql).await?;
        for (index, cell_dep) in tx.cell_deps().into_iter().enumerate() {
            let tmp: u32 = cell_dep.out_point().index().unpack();
            let dep_type: u8 = cell_dep.dep_type().into();
            txn.execute(
                &stmt,
                &[
                    &tx.hash().raw_data().as_ref(),
                    &(ref_index as i32),
                    &(index as i32),
                    &cell_dep.out_point().tx_hash().raw_data().as_ref(),
                    &(tmp as i32),
                    &(dep_type as i16),
                ],
            )
            .await?;
        }
    }
    {
        let sql = r#"
            INSERT INTO tx_header_deps (
                ref_tx_hash, ref_index, ref_dep_index, block_hash
            ) VALUES (
                $1, $2, $3, $4
            )
            ON CONFLICT DO NOTHING
        ;"#;
        let stmt = txn.prepare(sql).await?;
        for (index, header_dep) in tx.header_deps().into_iter().enumerate() {
            txn.execute(
                &stmt,
                &[
                    &tx.hash().raw_data().as_ref(),
                    &(ref_index as i32),
                    &(index as i32),
                    &header_dep.raw_data().as_ref(),
                ],
            )
            .await?;
        }
    }
    {
        let sql = r#"
            INSERT INTO tx_witnesses (
                ref_tx_hash, ref_index, ref_dep_index, witness
            ) VALUES (
                $1, $2, $3, $4
            )
            ON CONFLICT DO NOTHING
        ;"#;
        let stmt = txn.prepare(sql).await?;
        for (index, witness) in tx.witnesses().into_iter().enumerate() {
            txn.execute(
                &stmt,
                &[
                    &tx.hash().raw_data().as_ref(),
                    &(ref_index as i32),
                    &(index as i32),
                    &witness.raw_data().as_ref(),
                ],
            )
            .await?;
        }
    }
    let sql = r#"
        INSERT INTO transactions (
            hash, version
        ) VALUES (
            $1, $2
        )
        ON CONFLICT DO NOTHING
    ;"#;
    txn.execute(
        sql,
        &[&tx.hash().raw_data().as_ref(), &(tx.version() as i32)],
    )
    .await
    .map_err(Into::into)
}

pub(super) async fn remove_transaction(
    txn: &pg::Transaction<'_>,
    tx_hash: &packed::Byte32,
) -> Result<Vec<u64>> {
    log::trace!("remove transaction {:#}", tx_hash);
    let sqls = &[
        r#"DELETE FROM tx_cell_deps   WHERE ref_tx_hash = $1;"#,
        r#"DELETE FROM tx_header_deps WHERE ref_tx_hash = $1;"#,
        r#"DELETE FROM tx_witnesses   WHERE ref_tx_hash = $1;"#,
        r#"DELETE FROM transactions   WHERE        hash = $1;"#,
    ];
    let mut ret = Vec::with_capacity(sqls.len());
    for sql in sqls {
        let code = txn.execute(*sql, &[&tx_hash.raw_data().as_ref()]).await?;
        ret.push(code);
    }
    Ok(ret)
}

async fn insert_cell_data(
    txn: &pg::Transaction<'_>,
    data_hash: &packed::Byte32,
    data: &packed::Bytes,
) -> Result<u64> {
    log::trace!("insert cell data {:#}", data_hash);
    let sql = r#"
        INSERT INTO cells_data (
            hash, data
        ) VALUES (
            $1, $2
        )
        ON CONFLICT (hash) DO NOTHING
    ;"#;
    txn.execute(
        sql,
        &[&data_hash.raw_data().as_ref(), &data.raw_data().as_ref()],
    )
    .await
    .map_err(Into::into)
}

async fn remove_cell_data(txn: &pg::Transaction<'_>, data_hash: &packed::Byte32) -> Result<u64> {
    log::trace!("remove cell data {:#}", data_hash);
    let sql = r#"
        DELETE FROM cells_data cd
         WHERE 1 = 1
           AND hash = $1
           AND NOT EXISTS (
               SELECT 1
                 FROM cells c
                WHERE c.data_hash = cd.hash)
    ;"#;
    txn.execute(sql, &[&data_hash.raw_data().as_ref()])
        .await
        .map_err(Into::into)
}

async fn insert_script(
    txn: &pg::Transaction<'_>,
    script_hash: &packed::Byte32,
    script: &packed::Script,
) -> Result<u64> {
    log::trace!("insert script {:#}", script_hash);
    let sql = r#"
        INSERT INTO scripts (
            hash, code_hash, hash_type, args
        ) VALUES (
            $1, $2, $3, $4
        )
        ON CONFLICT (hash) DO NOTHING
    ;"#;
    let hash_type: u8 = script.hash_type().into();
    txn.execute(
        sql,
        &[
            &script_hash.raw_data().as_ref(),
            &script.code_hash().raw_data().as_ref(),
            &(hash_type as i16),
            &script.args().raw_data().as_ref(),
        ],
    )
    .await
    .map_err(Into::into)
}

async fn remove_script(txn: &pg::Transaction<'_>, script_hash: &packed::Byte32) -> Result<u64> {
    log::trace!("remove script {:#}", script_hash);
    let sql = r#"
        DELETE FROM scripts s
         WHERE 1 = 1
           AND hash = $1
           AND NOT EXISTS (
               SELECT 1
                 FROM cells c
                WHERE c.lock_hash = s.hash
                   OR c.type_hash = s.hash)
    ;"#;
    txn.execute(sql, &[&script_hash.raw_data().as_ref()])
        .await
        .map_err(Into::into)
}

pub(super) async fn insert_cells(
    txn: &pg::Transaction<'_>,
    tx_hash: &packed::Byte32,
    outputs: impl Iterator<Item = packed::CellOutput>,
    outputs_data: impl Iterator<Item = packed::Bytes>,
) -> Result<()> {
    log::trace!("insert cells for transaction {:#}", tx_hash);
    let sql = r#"
        INSERT INTO cells (
            tx_hash, index, capacity, lock_hash, type_hash, data_hash
        ) VALUES (
            $1, $2, $3, $4, $5, $6
        )
        ON CONFLICT DO NOTHING
    ;"#;
    let stmt = txn.prepare(sql).await?;
    for (index, (output, data)) in outputs.zip(outputs_data).enumerate() {
        let data_hash = packed::CellOutput::calc_data_hash(data.raw_data().as_ref());
        let lock_hash = output.lock().calc_script_hash();
        insert_cell_data(txn, &data_hash, &data).await?;
        insert_script(txn, &lock_hash, &output.lock()).await?;
        let capacity: core::Capacity = output.capacity().unpack();
        let type_hash_opt = if let Some(type_script) = output.type_().to_opt() {
            let type_hash = type_script.calc_script_hash();
            insert_script(txn, &type_hash, &type_script).await?;
            Some(type_hash)
        } else {
            None
        };
        txn.execute(
            &stmt,
            &[
                &tx_hash.raw_data().as_ref(),
                &(index as i32),
                &(capacity.as_u64() as i64),
                &lock_hash.raw_data().as_ref(),
                &type_hash_opt
                    .map(|type_hash| type_hash.raw_data())
                    .as_ref()
                    .map(AsRef::as_ref),
                &data_hash.raw_data().as_ref(),
            ],
        )
        .await?;
    }
    Ok(())
}

pub(super) async fn remove_cells(
    txn: &pg::Transaction<'_>,
    tx_hash: &packed::Byte32,
) -> Result<()> {
    log::trace!("remove cells for transaction {:#}", tx_hash);
    let sql = r#"
        DELETE FROM cells
         WHERE tx_hash = $1
     RETURNING data_hash, lock_hash, type_hash
    ;"#;
    let hashes = txn
        .query(sql, &[&tx_hash.raw_data().as_ref()])
        .await
        .map_err(Into::into)
        .and_then(|ref rows| {
            rows.iter()
                .map(|ref row| {
                    let data_hash = row
                        .try_get::<_, Vec<u8>>(0)
                        .map_err(Into::into)
                        .and_then(ops::hash_from_value)?;
                    let lock_hash = row
                        .try_get::<_, Vec<u8>>(1)
                        .map_err(Into::into)
                        .and_then(ops::hash_from_value)?;
                    let type_hash_opt = row
                        .try_get::<_, Option<Vec<u8>>>(2)?
                        .map(ops::hash_from_value)
                        .transpose()?;
                    Ok((data_hash, lock_hash, type_hash_opt))
                })
                .collect::<Result<Vec<(packed::Byte32, packed::Byte32, Option<packed::Byte32>)>>>()
        })?;
    for (data_hash, lock_hash, type_hash_opt) in hashes.into_iter() {
        remove_cell_data(txn, &data_hash).await?;
        remove_script(txn, &lock_hash).await?;
        if let Some(type_hash) = type_hash_opt {
            remove_script(txn, &type_hash).await?;
        }
    }
    Ok(())
}

pub(super) async fn consume_cells(
    txn: &pg::Transaction<'_>,
    consumed_tx_hash: &packed::Byte32,
    inputs: impl Iterator<Item = packed::CellInput>,
) -> Result<()> {
    log::trace!("consume cells for transaction {:#}", consumed_tx_hash);
    let sql = r#"
        UPDATE cells
           SET
               consumed_tx_hash = $1,
               consumed_index = $2,
               consumed_since = $3
         WHERE 1 = 1
           AND tx_hash = $4
           AND index = $5
    ;"#;
    let stmt = txn.prepare(sql).await?;
    for (consumed_index, input) in inputs.enumerate() {
        let since: u64 = input.since().unpack();
        let prev_output = input.previous_output();
        log::trace!("consume cell {:#}", prev_output);
        let tx_hash = prev_output.tx_hash();
        let index: u32 = prev_output.index().unpack();
        txn.execute(
            &stmt,
            &[
                &consumed_tx_hash.raw_data().as_ref(),
                &(consumed_index as i32),
                &(&since.to_le_bytes()[..]),
                &tx_hash.raw_data().as_ref(),
                &(index as i32),
            ],
        )
        .await?;
    }
    Ok(())
}

pub(super) async fn restore_cells(
    txn: &pg::Transaction<'_>,
    restored_tx_hash: &packed::Byte32,
) -> Result<u64> {
    log::trace!("restore cells for transaction {:#}", restored_tx_hash);
    let sql = r#"
        UPDATE cells
           SET
               consumed_tx_hash = null,
               consumed_index = null,
               consumed_since = null
         WHERE 1 = 1
           AND consumed_tx_hash = $1
    ;"#;
    txn.execute(sql, &[&restored_tx_hash.raw_data().as_ref()])
        .await
        .map_err(Into::into)
}
