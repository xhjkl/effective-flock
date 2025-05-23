use rand::rng;
use rand::seq::SliceRandom;
use rocksdb::{DB, Options, WriteBatch};
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::time::Instant;

use crate::common::{NUM_TABLES, READ_SAMPLE, ROWS_PER_TABLE};

/// RocksDB key-value benchmark
pub async fn run() -> Result<(), Box<dyn Error>> {
    println!("Running RocksDB k-v benchmark (table per CF)...");

    // Prepare RocksDB
    let db_path = ".data";
    let _ = fs::remove_dir_all(db_path);
    let mut opts = Options::default();
    opts.create_if_missing(true);
    let mut db = DB::open(&opts, db_path)?;

    // Create column families for each table
    for table_idx in 0..NUM_TABLES {
        let cf_name = format!("kv_{}", table_idx);
        if db.cf_handle(&cf_name).is_none() {
            db.create_cf(&cf_name, &opts)?;
        }
    }

    // Populate each table
    let start_pop = Instant::now();
    for table_idx in 0..NUM_TABLES {
        let cf = db.cf_handle(&format!("kv_{}", table_idx)).unwrap();
        let mut batch = WriteBatch::default();
        for row in 1..=ROWS_PER_TABLE as i32 {
            let key = row.to_be_bytes();
            let val = format!("value{}_{}", table_idx, row);
            batch.put_cf(cf, key, val.as_bytes());
        }
        db.write(batch)?;
    }
    println!("RocksDB preparation took: {:?}", start_pop.elapsed());

    // Generate random read pattern across tables
    let mut read_keys: Vec<(usize, Vec<u8>)> = (0..NUM_TABLES)
        .flat_map(|table_idx| {
            (1..=ROWS_PER_TABLE as i32).map(move |row| (table_idx, row.to_be_bytes().to_vec()))
        })
        .collect();
    let mut rng = rng();
    read_keys.shuffle(&mut rng);
    // Only read a small sample to match Postgres side
    read_keys.truncate(READ_SAMPLE);

    // Group keys by table index
    let mut groups: HashMap<usize, Vec<Vec<u8>>> = HashMap::new();
    for (table_idx, key_bytes) in &read_keys {
        groups
            .entry(*table_idx)
            .or_default()
            .push(key_bytes.clone());
    }

    // Warmup run (not timed)
    for (table_idx, keys) in &groups {
        let cf = db.cf_handle(&format!("kv_{}", table_idx)).unwrap();
        let results = db.batched_multi_get_cf(cf, keys, false);
        for res in results {
            let _ = res?;
        }
    }
    // Then batch reads per table
    const MEASUREMENT_ROUNDS: usize = 5;
    for i in 1..=MEASUREMENT_ROUNDS {
        let start = Instant::now();
        for (table_idx, keys) in &groups {
            let cf = db.cf_handle(&format!("kv_{}", table_idx)).unwrap();
            let results = db.batched_multi_get_cf(cf, keys, false);
            for res in results {
                let _ = res?;
            }
        }
        println!("Run {} completed in: {:?}", i, start.elapsed());
    }
    Ok(())
}
