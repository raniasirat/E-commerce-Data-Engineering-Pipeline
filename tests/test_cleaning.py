import pandas as pd
from src.processing import cleaning


def test_standardize_columns():
    df = pd.DataFrame(columns=[" Order ID ", "Price-USD"])
    assert list(cleaning.standardize_columns(df).columns) == ["order_id", "price_usd"]


def test_dates_keep_legit_nulls():
    df = pd.DataFrame({"d": ["2018-01-01 10:00:00", None, "oops"]})
    out = cleaning.convert_dates(df, ["d"], "t")
    assert out["d"].isna().sum() == 2


def test_dedup_by_key():
    df = pd.DataFrame({"order_id": ["a", "a", "b"], "x": [1, 2, 3]})
    clean, rej = cleaning.drop_duplicates_logged(df, ["order_id"], "t")
    assert len(clean) == 2 and len(rej) == 1
