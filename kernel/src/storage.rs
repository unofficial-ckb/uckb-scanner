// Copyright (C) 2019 Boyu Yang
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use property::Property;

use uckb_jsonrpc_interfaces::types::{core, packed, prelude::*};

use crate::error::{Error, Result};

#[derive(Property)]
pub struct Storage {
    conn: postgres::Connection,
}

impl Storage {
    pub fn connect(uri: &str) -> Result<Self> {
        Ok(
            postgres::Connection::connect(uri, postgres::TlsMode::None)
                .map(|conn| Self { conn })?,
        )
    }

    pub fn initialize(&self) -> Result<Option<u64>> {
        if self.is_first_run()? {
            self.create_tables()?;
        }
        self.check_current_block()
    }

    pub fn destory(&self) -> Result<()> {
        self.drop_tables()
    }

    pub fn insert_block(&self, block: &core::BlockView) -> Result<()> {
        if block.number() != 0 && !self.verify_block(block)? {
            return Err(Error::UnknownParentBlock(
                block.number(),
                block.parent_hash().unpack(),
            ));
        }
        self.insert_block_header(block)?;
        for (index, uncle) in block.uncles().into_iter().enumerate() {
            self.insert_uncle_header(&uncle)?;
            self.insert_block_uncle(&block.hash(), &uncle.hash(), index as i32)?;
            for (index, proposal) in uncle.data().proposals().into_iter().enumerate() {
                self.insert_block_proposal(&uncle.hash(), &proposal, index as i32)?;
            }
        }
        for (index, proposal) in block.data().proposals().into_iter().enumerate() {
            self.insert_block_proposal(&block.hash(), &proposal, index as i32)?;
        }
        for (tx_index, tx) in block.transactions().into_iter().enumerate() {
            self.insert_block_transaction(&block.hash(), &tx.hash(), tx_index as i32)?;
            self.insert_transaction(&tx, tx_index as i32)?;
            if tx_index != 0 {
                for (input_index, input) in tx.data().raw().inputs().into_iter().enumerate() {
                    self.consume_cell(&tx.hash(), input_index as i32, &input)?;
                }
            }
            for (output_index, (output, data)) in tx
                .data()
                .raw()
                .outputs()
                .into_iter()
                .zip(tx.data().raw().outputs_data().into_iter())
                .enumerate()
            {
                self.insert_cell(&tx.hash(), output_index as i32, &output, &data)?;
            }
        }
        Ok(())
    }

    fn is_first_run(&self) -> Result<bool> {
        self.conn()
            .query("SELECT 1 FROM block_headers;", &[])
            .map(|_| false)
            .or_else(|err| {
                if err
                    .code()
                    .map(|s| *s == postgres::error::UNDEFINED_TABLE)
                    .unwrap_or(false)
                {
                    Ok(true)
                } else {
                    Err(err)
                }
            })
            .map_err(Into::into)
    }

    fn check_current_block(&self) -> Result<Option<u64>> {
        let records = self
            .conn()
            .query("SELECT COALESCE(MAX(number), -1) FROM block_headers;", &[])?;
        let record = records.get(0);
        let number: i64 = record.get(0);
        if number == -1 {
            Ok(None)
        } else {
            Ok(Some(number as u64))
        }
    }

    fn create_tables(&self) -> Result<()> {
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
                dao_ar              BYTEA       NOT NULL,
                dao_s               BIGINT      NOT NULL,
                dao_u               BIGINT      NOT NULL,
                nonce               BYTEA       NOT NULL
            );"#;
        self.execute(sql)?;
        let sql = r#"
            CREATE TABLE IF NOT EXISTS block_uncles (
                block_hash          BYTEA       NOT NULL,
                uncle_hash          BYTEA       NOT NULL,
                index               INTEGER     NOT NULL,
                PRIMARY KEY (block_hash, uncle_hash)
            );"#;
        self.execute(sql)?;
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
                dao_ar              BYTEA       NOT NULL,
                dao_s               BIGINT      NOT NULL,
                dao_u               BIGINT      NOT NULL,
                nonce               BYTEA       NOT NULL
            );"#;
        self.execute(sql)?;
        let sql = r#"
            CREATE TABLE IF NOT EXISTS block_proposals (
                block_hash          BYTEA       NOT NULL,
                short_id            BYTEA       NOT NULL,
                index               INTEGER     NOT NULL,
                PRIMARY KEY (block_hash, short_id)
            );"#;
        self.execute(sql)?;
        let sql = r#"
            CREATE TABLE IF NOT EXISTS block_transactions (
                block_hash          BYTEA       NOT NULL,
                tx_hash             BYTEA       NOT NULL,
                index               INTEGER     NOT NULL,
                PRIMARY KEY (block_hash, tx_hash)
            );"#;
        self.execute(sql)?;
        let sql = r#"
            CREATE TABLE IF NOT EXISTS transactions (
                hash                BYTEA       NOT NULL PRIMARY KEY,
                version             INTEGER     NOT NULL
            );"#;
        self.execute(sql)?;
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
        self.execute(sql)?;
        let sql = r#"
            CREATE TABLE IF NOT EXISTS tx_header_deps (
                ref_tx_hash         BYTEA       NOT NULL,
                ref_index           INTEGER     NOT NULL,
                ref_dep_index       INTEGER     NOT NULL,
                block_hash          BYTEA       NOT NULL,
                PRIMARY KEY (ref_tx_hash, ref_index, ref_dep_index)
            );"#;
        self.execute(sql)?;
        let sql = r#"
            CREATE TABLE IF NOT EXISTS tx_witnesses (
                ref_tx_hash         BYTEA       NOT NULL,
                ref_index           INTEGER     NOT NULL,
                ref_dep_index       INTEGER     NOT NULL,
                witness             BYTEA       NOT NULL,
                PRIMARY KEY (ref_tx_hash, ref_index, ref_dep_index)
            );"#;
        self.execute(sql)?;
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
        self.execute(sql)?;
        let sql = r#"
            CREATE TABLE IF NOT EXISTS cells_data (
                hash                BYTEA       NOT NULL PRIMARY KEY,
                data                BYTEA       NOT NULL
            );"#;
        self.execute(sql)?;
        let sql = r#"
            CREATE TABLE IF NOT EXISTS scripts (
                hash                BYTEA       NOT NULL PRIMARY KEY,
                code_hash           BYTEA       NOT NULL,
                hash_type           SMALLINT    NOT NULL,
                args                BYTEA       NOT NULL
            );"#;
        self.execute(sql)?;
        Ok(())
    }

    fn drop_tables(&self) -> Result<()> {
        self.drop_table("block_headers")?;
        self.drop_table("block_uncles")?;
        self.drop_table("uncle_headers")?;
        self.drop_table("block_proposals")?;
        self.drop_table("block_transactions")?;
        self.drop_table("transactions")?;
        self.drop_table("tx_cell_deps")?;
        self.drop_table("tx_header_deps")?;
        self.drop_table("tx_witnesses")?;
        self.drop_table("cells")?;
        self.drop_table("cells_data")?;
        self.drop_table("scripts")?;
        Ok(())
    }

    fn drop_table(&self, table: &str) -> Result<()> {
        let sql = format!("DROP TABLE IF EXISTS {};", table);
        self.execute(&sql)
    }

    fn execute(&self, sql: &str) -> Result<()> {
        self.conn()
            .execute(sql, &[])
            .map(|_| ())
            .map_err(Into::into)
    }

    fn insert_block_header(&self, block: &core::BlockView) -> Result<()> {
        let sql = r#"
            INSERT INTO block_headers (
                hash, version, compact_target, timestamp,
                number, epoch_number, epoch_index, epoch_length,
                parent_hash, transactions_root, proposals_hash, uncles_hash,
                dao_c, dao_ar, dao_s, dao_u, nonce
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17
            )
            ON CONFLICT DO NOTHING;"#;
        let nonce: u128 = block.nonce();
        let (dao_c, dao_ar, dao_s, dao_u) = extract_dao(block.dao().raw_data().as_ref());
        self.conn()
            .execute(
                sql,
                &[
                    &block.hash().raw_data().as_ref(),
                    &(block.version() as i32),
                    &(block.compact_target() as i64),
                    &(block.timestamp() as i64),
                    &(block.number() as i64),
                    &(block.epoch().number() as i32),
                    &(block.epoch().index() as i32),
                    &(block.epoch().length() as i32),
                    &block.parent_hash().raw_data().as_ref(),
                    &block.transactions_root().raw_data().as_ref(),
                    &block.proposals_hash().raw_data().as_ref(),
                    &block.uncles_hash().raw_data().as_ref(),
                    &(dao_c as i64),
                    &(&dao_ar.to_le_bytes()[..]),
                    &(dao_s as i64),
                    &(dao_u as i64),
                    &(&nonce.to_le_bytes()[..]),
                ],
            )
            .map(|_| ())
            .map_err(Into::into)
    }

    fn insert_block_uncle(
        &self,
        block_hash: &packed::Byte32,
        uncle_hash: &packed::Byte32,
        index: i32,
    ) -> Result<()> {
        let sql = r#"
            INSERT INTO block_uncles (
                block_hash, uncle_hash, index
            ) VALUES (
                $1, $2, $3
            )
            ON CONFLICT DO NOTHING;"#;
        self.conn()
            .execute(
                sql,
                &[
                    &block_hash.raw_data().as_ref(),
                    &uncle_hash.raw_data().as_ref(),
                    &index,
                ],
            )
            .map(|_| ())
            .map_err(Into::into)
    }

    fn insert_uncle_header(&self, uncle: &core::UncleBlockView) -> Result<()> {
        let sql = r#"
            INSERT INTO uncle_headers (
                hash, version, compact_target, timestamp,
                number, epoch_number, epoch_index, epoch_length,
                parent_hash, transactions_root, proposals_hash, uncles_hash,
                dao_c, dao_ar, dao_s, dao_u, nonce
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17
            )
            ON CONFLICT DO NOTHING;"#;
        let (dao_c, dao_ar, dao_s, dao_u) = extract_dao(uncle.dao().raw_data().as_ref());
        self.conn()
            .execute(
                sql,
                &[
                    &uncle.hash().raw_data().as_ref(),
                    &(uncle.version() as i32),
                    &(uncle.compact_target() as i64),
                    &(uncle.timestamp() as i64),
                    &(uncle.number() as i64),
                    &(uncle.epoch().number() as i32),
                    &(uncle.epoch().index() as i32),
                    &(uncle.epoch().length() as i32),
                    &uncle.parent_hash().raw_data().as_ref(),
                    &uncle.transactions_root().raw_data().as_ref(),
                    &uncle.proposals_hash().raw_data().as_ref(),
                    &uncle.uncles_hash().raw_data().as_ref(),
                    &(dao_c as i64),
                    &(&dao_ar.to_le_bytes()[..]),
                    &(dao_s as i64),
                    &(dao_u as i64),
                    &uncle.data().header().nonce().raw_data().as_ref(),
                ],
            )
            .map(|_| ())
            .map_err(Into::into)
    }

    fn insert_block_proposal(
        &self,
        block_hash: &packed::Byte32,
        proposal: &packed::ProposalShortId,
        index: i32,
    ) -> Result<()> {
        let sql = r#"
            INSERT INTO block_proposals (
                block_hash, short_id, index
            ) VALUES (
                $1, $2, $3
            )
            ON CONFLICT DO NOTHING;"#;
        self.conn()
            .execute(
                sql,
                &[
                    &block_hash.raw_data().as_ref(),
                    &proposal.raw_data().as_ref(),
                    &index,
                ],
            )
            .map(|_| ())
            .map_err(Into::into)
    }

    fn insert_block_transaction(
        &self,
        block_hash: &packed::Byte32,
        tx_hash: &packed::Byte32,
        index: i32,
    ) -> Result<()> {
        let sql = r#"
            INSERT INTO block_transactions (
                block_hash, tx_hash, index
            ) VALUES (
                $1, $2, $3
            )
            ON CONFLICT DO NOTHING;"#;
        self.conn()
            .execute(
                sql,
                &[
                    &block_hash.raw_data().as_ref(),
                    &tx_hash.raw_data().as_ref(),
                    &index,
                ],
            )
            .map(|_| ())
            .map_err(Into::into)
    }

    fn insert_transaction(&self, tx: &core::TransactionView, ref_index: i32) -> Result<()> {
        for (index, cell_dep) in tx.cell_deps().into_iter().enumerate() {
            let sql = r#"
                INSERT INTO tx_cell_deps (
                    ref_tx_hash, ref_index, ref_dep_index, tx_hash, index, dep_type
                ) VALUES (
                    $1, $2, $3, $4, $5, $6
                )
                ON CONFLICT DO NOTHING;"#;
            let tmp: u32 = cell_dep.out_point().index().unpack();
            let dep_type: u8 = cell_dep.dep_type().into();
            self.conn()
                .execute(
                    sql,
                    &[
                        &tx.hash().raw_data().as_ref(),
                        &ref_index,
                        &(index as i32),
                        &cell_dep.out_point().tx_hash().raw_data().as_ref(),
                        &(tmp as i32),
                        &(dep_type as i16),
                    ],
                )
                .map(|_| ())?;
        }
        for (index, header_dep) in tx.header_deps().into_iter().enumerate() {
            let sql = r#"
                INSERT INTO tx_header_deps (
                    ref_tx_hash, ref_index, ref_dep_index, block_hash
                ) VALUES (
                    $1, $2, $3, $4
                )
                ON CONFLICT DO NOTHING;"#;
            self.conn()
                .execute(
                    sql,
                    &[
                        &tx.hash().raw_data().as_ref(),
                        &ref_index,
                        &(index as i32),
                        &header_dep.raw_data().as_ref(),
                    ],
                )
                .map(|_| ())?;
        }
        for (index, witness) in tx.witnesses().into_iter().enumerate() {
            let sql = r#"
                INSERT INTO tx_witnesses (
                    ref_tx_hash, ref_index, ref_dep_index, witness
                ) VALUES (
                    $1, $2, $3, $4
                )
                ON CONFLICT DO NOTHING;"#;
            self.conn()
                .execute(
                    sql,
                    &[
                        &tx.hash().raw_data().as_ref(),
                        &ref_index,
                        &(index as i32),
                        &witness.raw_data().as_ref(),
                    ],
                )
                .map(|_| ())?;
        }
        let sql = r#"
            INSERT INTO transactions (
                hash, version
            ) VALUES (
                $1, $2
            )
            ON CONFLICT DO NOTHING;"#;
        self.conn()
            .execute(
                sql,
                &[&tx.hash().raw_data().as_ref(), &(tx.version() as i32)],
            )
            .map(|_| ())
            .map_err(Into::into)
    }

    fn insert_cell(
        &self,
        tx_hash: &packed::Byte32,
        index: i32,
        output: &packed::CellOutput,
        data: &packed::Bytes,
    ) -> Result<()> {
        let data_hash = self.insert_cell_data(data)?;
        let lock_hash = self.insert_script(&output.lock())?;
        let capacity: core::Capacity = output.capacity().unpack();
        if let Some(type_script) = output.type_().to_opt() {
            let type_hash = self.insert_script(&type_script)?;
            let sql = r#"
                INSERT INTO cells (
                    tx_hash, index, capacity, lock_hash, type_hash, data_hash
                ) VALUES (
                    $1, $2, $3, $4, $5, $6
                )
                ON CONFLICT DO NOTHING;"#;
            self.conn().execute(
                sql,
                &[
                    &tx_hash.raw_data().as_ref(),
                    &index,
                    &(capacity.as_u64() as i64),
                    &lock_hash.raw_data().as_ref(),
                    &type_hash.raw_data().as_ref(),
                    &data_hash.raw_data().as_ref(),
                ],
            )
        } else {
            let sql = r#"
                INSERT INTO cells (
                    tx_hash, index, capacity, lock_hash, data_hash
                ) VALUES (
                    $1, $2, $3, $4, $5
                )
                ON CONFLICT DO NOTHING;"#;
            self.conn().execute(
                sql,
                &[
                    &tx_hash.raw_data().as_ref(),
                    &index,
                    &(capacity.as_u64() as i64),
                    &lock_hash.raw_data().as_ref(),
                    &data_hash.raw_data().as_ref(),
                ],
            )
        }
        .map(|_| ())
        .map_err(Into::into)
    }

    fn consume_cell(
        &self,
        consumed_tx_hash: &packed::Byte32,
        consumed_index: i32,
        input: &packed::CellInput,
    ) -> Result<()> {
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
        let since: u64 = input.since().unpack();
        let tx_hash = input.previous_output().tx_hash();
        let index: u32 = input.previous_output().index().unpack();
        self.conn()
            .execute(
                sql,
                &[
                    &consumed_tx_hash.raw_data().as_ref(),
                    &consumed_index,
                    &(&since.to_le_bytes()[..]),
                    &tx_hash.raw_data().as_ref(),
                    &(index as i32),
                ],
            )
            .map(|_| ())
            .map_err(Into::into)
    }

    fn insert_cell_data(&self, data: &packed::Bytes) -> Result<packed::Byte32> {
        let hash = packed::CellOutput::calc_data_hash(data.raw_data().as_ref());
        let sql = r#"
            INSERT INTO cells_data (
                hash, data
            ) VALUES (
                $1, $2
            )
            ON CONFLICT (hash) DO NOTHING;"#;
        self.conn()
            .execute(sql, &[&hash.raw_data().as_ref(), &data.raw_data().as_ref()])
            .map(|_| hash)
            .map_err(Into::into)
    }

    fn insert_script(&self, script: &packed::Script) -> Result<packed::Byte32> {
        let hash = script.calc_script_hash();
        let sql = r#"
            INSERT INTO scripts (
                hash, code_hash, hash_type, args
            ) VALUES (
                $1, $2, $3, $4
            )
            ON CONFLICT (hash) DO NOTHING;"#;
        let hash_type: u8 = script.hash_type().into();
        self.conn()
            .execute(
                sql,
                &[
                    &hash.raw_data().as_ref(),
                    &script.code_hash().raw_data().as_ref(),
                    &(hash_type as i16),
                    &script.args().raw_data().as_ref(),
                ],
            )
            .map(|_| hash)
            .map_err(Into::into)
    }

    fn verify_block(&self, block: &core::BlockView) -> Result<bool> {
        let sql = r#"
            SELECT 1
              FROM block_headers
             WHERE 1 = 1
               AND number = $1
               AND hash = $2
        "#;
        self.conn()
            .query(
                sql,
                &[
                    &((block.number() - 1) as i64),
                    &(block.parent_hash().raw_data().as_ref()),
                ],
            )
            .map(|records| !records.is_empty())
            .map_err(Into::into)
    }
}

fn extract_dao(slice: &[u8]) -> (u64, u64, u64, u64) {
    let mut tmp = [0u8; 8];
    tmp.copy_from_slice(&slice[0..8]);
    let dao_c = u64::from_le_bytes(tmp);
    tmp.copy_from_slice(&slice[8..16]);
    let dao_ar = u64::from_le_bytes(tmp);
    tmp.copy_from_slice(&slice[16..24]);
    let dao_s = u64::from_le_bytes(tmp);
    tmp.copy_from_slice(&slice[24..32]);
    let dao_u = u64::from_le_bytes(tmp);
    (dao_c, dao_ar, dao_s, dao_u)
}
