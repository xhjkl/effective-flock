mod common;

mod kv;
mod postgres;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Running Postgres benchmark...");
    postgres::run().await?;

    println!("Running k-v benchmark...");
    kv::run().await?;

    Ok(())
}
