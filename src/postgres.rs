use std::time::Instant;
use tokio_postgres::Client;
use tokio_postgres::NoTls;

use crate::common::NUM_TABLES;
use crate::common::ROWS_PER_TABLE;

async fn prepare_cat_tables_if_needed(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    let tables: Vec<String> = (0..NUM_TABLES)
        .map(|i| format!("cat_value_{}", i))
        .collect();
    // dynamically generate cat columns: cat_1..=NUM_TABLES
    let cat_cols: Vec<String> = (1..=NUM_TABLES as i32)
        .map(|n| format!("cat_{}", n))
        .collect();
    for table in &tables {
        // check if table exists
        let row = client
            .query_one(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema='public' AND table_name = $1)",
                &[table],
            )
            .await?;
        let exists: bool = row.get(0);
        if exists {
            println!("Table {} exists, skipping preparation", table);
            continue;
        }
        println!("Creating and populating table {}", table);
        // create table with ext_id and category columns
        let cols_defs = std::iter::once("ext_id TEXT NOT NULL".to_string())
            .chain(cat_cols.iter().map(|col| format!("{} TEXT", col)))
            .collect::<Vec<_>>()
            .join(", ");
        let create_stmt = format!("CREATE TABLE {} ({})", table, cols_defs);
        client.execute(&create_stmt, &[]).await?;
        // populate with placeholder rows
        for i in 1..=ROWS_PER_TABLE as i32 {
            let ext_id = format!("ext_{}", i);
            let cols = std::iter::once("ext_id")
                .chain(cat_cols.iter().map(|c| c.as_str()))
                .collect::<Vec<_>>();
            let cols_str = cols.join(", ");
            let vals = std::iter::once(format!("'{}'", ext_id))
                .chain(cat_cols.iter().map(|col| format!("'{}_{}'", col, i)))
                .collect::<Vec<_>>()
                .join(", ");
            let insert_stmt = format!("INSERT INTO {} ({}) VALUES ({})", table, cols_str, vals);
            client.execute(&insert_stmt, &[]).await?;
            // create index on ext_id for faster lookups
            let idx_stmt = format!(
                "CREATE INDEX IF NOT EXISTS idx_{}_ext_id ON {} (ext_id)",
                table, table
            );
            client.execute(&idx_stmt, &[]).await?;
        }
    }
    Ok(())
}

async fn prepare_feat_tables_if_needed(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    let tables: Vec<String> = (0..NUM_TABLES)
        .map(|i| format!("feat_value_{}", i))
        .collect();
    // dynamically generate feat columns: feat_1..=NUM_TABLES
    let feat_cols: Vec<String> = (1..=NUM_TABLES as i32)
        .map(|n| format!("feat_{}", n))
        .collect();
    for table in &tables {
        let row = client
            .query_one(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema='public' AND table_name = $1)",
                &[table],
            )
            .await?;
        let exists: bool = row.get(0);
        if exists {
            println!("Table {} exists, skipping preparation", table);
            continue;
        }
        println!("Creating and populating table {}", table);
        let cols_defs = std::iter::once("ext_id TEXT NOT NULL".to_string())
            .chain(
                feat_cols
                    .iter()
                    .map(|col| format!("{} DOUBLE PRECISION", col)),
            )
            .collect::<Vec<_>>()
            .join(", ");
        let create_stmt = format!("CREATE TABLE {} ({})", table, cols_defs);
        client.execute(&create_stmt, &[]).await?;

        for i in 1..=ROWS_PER_TABLE as i32 {
            let ext_id = format!("ext_{}", i);
            let cols = std::iter::once("ext_id")
                .chain(feat_cols.iter().map(|c| c.as_str()))
                .collect::<Vec<_>>();
            let cols_str = cols.join(", ");
            let vals = std::iter::once(format!("'{}'", ext_id))
                .chain(feat_cols.iter().map(|_| format!("{}", i)))
                .collect::<Vec<_>>()
                .join(", ");
            let insert_stmt = format!("INSERT INTO {} ({}) VALUES ({})", table, cols_str, vals);
            client.execute(&insert_stmt, &[]).await?;
            // create index on ext_id for faster lookups
            let idx_stmt = format!(
                "CREATE INDEX IF NOT EXISTS idx_{}_ext_id ON {} (ext_id)",
                table, table
            );
            client.execute(&idx_stmt, &[]).await?;
        }
    }
    Ok(())
}

/// Executes one combined cat and feat query round
async fn complex_roundtrip(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    // 1) collect all ext_id from cat tables
    let cat_tables: Vec<String> = (0..NUM_TABLES)
        .map(|i| format!("cat_value_{}", i))
        .collect();
    // constrained cat selects in a single UNION ALL query using sample ext_ids fetched from DB
    let sample_rows = client
        .query("SELECT ext_id FROM cat_value_1 LIMIT 4", &[])
        .await?;
    let sample_ids: Vec<String> = sample_rows.iter().map(|r| r.get(0)).collect();
    let sample_ids_str = sample_ids
        .iter()
        .map(|e| format!("'{}'", e))
        .collect::<Vec<_>>()
        .join(", ");
    let cat_queries = cat_tables
        .iter()
        .map(|table| {
            format!(
                "SELECT ext_id FROM {} WHERE ext_id IN ({})",
                table, sample_ids_str
            )
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    // execute cat queries and collect returned ext_ids for feat filtering
    let cat_rows = client.query(&cat_queries, &[]).await?;
    let ext_ids_from_cats: Vec<String> = cat_rows.iter().map(|r| r.get(0)).collect();
    let ext_ids_str = ext_ids_from_cats
        .iter()
        .map(|e| format!("'{}'", e))
        .collect::<Vec<_>>()
        .join(", ");

    // 2) batch feat selects into one query
    let feat_tables: Vec<String> = (0..NUM_TABLES)
        .map(|i| format!("feat_value_{}", i))
        .collect();
    let feat_cols: &[&str] = &["feat_1", "feat_2", "feat_3"];
    let sub_queries: Vec<String> = feat_tables
        .iter()
        .map(|table| {
            let cols = std::iter::once("ext_id")
                .chain(feat_cols.iter().cloned())
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "SELECT {} FROM {} WHERE ext_id IN ({})",
                cols, table, ext_ids_str
            )
        })
        .collect();
    let feat_query = sub_queries.join(" UNION ALL ");
    let _ = client.query(&feat_query, &[]).await?;

    Ok(())
}

const MEASUREMENT_ROUNDS: usize = 5;
/// Runs warmup then repeats the combined query multiple times
async fn run_complex(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    // warmup
    complex_roundtrip(client).await?;
    complex_roundtrip(client).await?;

    // timed runs
    for i in 1..=MEASUREMENT_ROUNDS {
        let start = Instant::now();
        complex_roundtrip(client).await?;
        println!("Run {} completed in: {:?}", i, start.elapsed());
    }
    Ok(())
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("Running Postgres benchmarks...");

    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL");
    let (client, conn) = tokio_postgres::connect(&database_url, NoTls)
        .await
        .expect("postgres");
    tokio::spawn(async move {
        conn.await.expect("postgres");
    });

    // Ensure clean slate: drop existing cat and feat tables
    for i in 0..NUM_TABLES {
        let drop_cat = format!("DROP TABLE IF EXISTS cat_value_{}", i);
        client.execute(&drop_cat, &[]).await?;
        let drop_feat = format!("DROP TABLE IF EXISTS feat_value_{}", i);
        client.execute(&drop_feat, &[]).await?;
    }

    // Preparation phase: create and populate cat_value_* tables if missing
    prepare_cat_tables_if_needed(&client).await?;
    // Preparation phase: create and populate feat_value_* tables if missing
    prepare_feat_tables_if_needed(&client).await?;

    // Complex benchmark: warmup + repeated combined cat+feat queries
    run_complex(&client).await?;

    Ok(())
}
