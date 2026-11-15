import pytest

pytest.importorskip("pyspark")

from src.processing import spark_transformations as T
from src.processing.spark_session import get_spark


@pytest.fixture(scope="module")
def spark():
    session = get_spark("tests")
    yield session
    session.stop()


def test_clean_orders_dedup_and_dates(spark):
    df = spark.createDataFrame(
        [
            ("o1", "delivered", "2018-01-01 10:00:00"),
            ("o1", "delivered", "2018-01-01 10:00:00"),
            ("o2", "canceled", None),
        ],
        ["order_id", "order_status", "order_purchase_timestamp"],
    )
    cfg = {
        "key": ["order_id"],
        "timestamps": ["order_purchase_timestamp"],
        "ints": [],
        "doubles": [],
    }

    out = T.clean_table(df, cfg)

    assert out.count() == 2
    assert dict(out.dtypes)["order_purchase_timestamp"] == "timestamp"


def test_filter_valid_items(spark):
    df = spark.createDataFrame(
        [(10.0, 1.0), (-5.0, 1.0), (None, 2.0)],
        "price double, freight_value double",
    )

    assert T.filter_valid_order_items(df).count() == 1
