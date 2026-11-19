//! Not-found errors name the closest existing name and the available ones (D30).

use std::sync::Arc;

use moraine::expr::{col, lit};
use moraine::{Database, Error, NotFound, TableOptions};
use moraine_testkit::SimRng;
use moraine_testkit::datagen;
use object_store::memory::InMemory;

#[tokio::test]
async fn misspelt_names_get_suggestions() {
    let db = Database::builder_for_store(Arc::new(InMemory::new()), "db")
        .open()
        .await
        .unwrap();
    for name in ["metrics", "params", "runs"] {
        db.create_table(name, datagen::row_schema(), TableOptions::default())
            .await
            .unwrap();
    }

    let Err(Error::TableNotFound(nf)) = db.open_table("metrcs").await else {
        panic!("expected TableNotFound");
    };
    assert_eq!(nf, NotFound::among("metrcs", ["metrics", "params", "runs"]));
    assert_eq!(nf.suggestion.as_deref(), Some("metrics"));
    assert_eq!(
        Error::TableNotFound(nf).to_string(),
        r#"table not found: "metrcs"; did you mean "metrics"?; available: metrics, params, runs"#
    );
    let Err(Error::TableNotFound(nf)) = db.open_table("blobs").await else {
        panic!("expected TableNotFound");
    };
    assert_eq!(nf.suggestion, None);
    assert_eq!(nf.available.len(), 3);

    let t = db.open_table("runs").await.unwrap();
    t.append(vec![datagen::rows(&mut SimRng::new(1), "a", 4, None, 2)])
        .await
        .unwrap();
    let column = |e: Error| match e {
        Error::ColumnNotFound(nf) => nf,
        other => panic!("expected ColumnNotFound, got {other}"),
    };
    let all = ["g", "i", "k", "op"].map(String::from).to_vec();

    let nf = column(t.scan().select(["opp"]).collect().await.unwrap_err());
    assert_eq!(nf.suggestion.as_deref(), Some("op"));
    assert_eq!(nf.available, all);
    let nf = column(
        t.scan()
            .filter(col("kk").eq(lit("x")))
            .count()
            .await
            .unwrap_err(),
    );
    assert_eq!(nf.suggestion.as_deref(), Some("k"));
    let nf = column(t.delete(col("gg").eq(lit(1i64))).await.unwrap_err());
    assert_eq!(nf.suggestion.as_deref(), Some("g"));
    let nf = column(
        t.update(col("g").eq(lit(1i64)), vec![("ii".into(), lit(0i64))])
            .await
            .unwrap_err(),
    );
    assert_eq!(nf.suggestion.as_deref(), Some("i"));
    let nf = column(
        t.update(col("j").eq(lit(1i64)), vec![("i".into(), lit(0i64))])
            .await
            .unwrap_err(),
    );
    assert_eq!(nf.name, "j");
    assert_eq!(nf.suggestion.as_deref(), Some("g"));
}
