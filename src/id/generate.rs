use std::collections::VecDeque;
use std::sync::Arc;

use futures_util::lock::Mutex;
use md5::{Digest, Md5};
use sqlx::PgPool;

const TABLE_NAME: &str = "id_generate";

#[derive(Debug)]
struct InnerGenerator {
    db_pool: PgPool,
    id_type: String,
    id_list: VecDeque<String>,
    step: i8,
}

#[derive(Debug, Clone)]
pub struct Generator {
    inner: Arc<Mutex<InnerGenerator>>,
}

impl Generator {
    pub async fn new(db: &PgPool, id_type: &str) -> anyhow::Result<Self> {
        sqlx::query(&format!("select from {} where id_type = $1", TABLE_NAME))
            .bind(id_type)
            .execute(db)
            .await?;

        let step = 10i8;

        Ok(Self {
            inner: Arc::new(Mutex::new(InnerGenerator {
                db_pool: db.clone(),
                id_type: id_type.to_owned(),
                id_list: VecDeque::with_capacity(step as _),
                step,
            })),
        })
    }

    pub async fn get_id(&self) -> anyhow::Result<String> {
        let mut inner = self.inner.lock().await;

        if let Some(id) = inner.id_list.pop_front() {
            return Ok(id);
        }

        let (max_id, ) = sqlx::query_as::<_, (i64, )>(&format!(
            "update {} set id_value=id_value+$1 where id_type=$2 returning id_value",
            TABLE_NAME
        ))
            .bind(inner.step as i32)
            .bind(&inner.id_type)
            .fetch_one(&inner.db_pool)
            .await?;

        let start_id: i64 = max_id - (inner.step as i64) - 1;

        let mut hasher = Md5::new();

        (start_id..=max_id).for_each(|id| {
            hasher.update(id.to_be_bytes());

            let id = hex::encode(hasher.finalize_reset())
                .chars()
                .take(10)
                .collect::<String>();

            inner.id_list.push_back(id);
        });

        Ok(inner.id_list.pop_front().unwrap())
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use sqlx::postgres::PgPoolOptions;

    use super::*;

    #[tokio::test]
    async fn get_id() {
        let pg_uri = env::var("PG_URI").expect("must set environment PG_URI");
        let id_type = env::var("ID_TYPE").expect("must set environment ID_TYPE");

        let pg_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&pg_uri)
            .await
            .unwrap();

        let generator = Generator::new(&pg_pool, &id_type).await.unwrap();

        println!("id is {}", generator.get_id().await.unwrap());
    }
}
