use image_bed::run;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    run().await
}