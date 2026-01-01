import math
import datetime

from dateutil.parser import parse
import polars as pl


def diff_rate(
    new_count: int,
    old_count: int,
) -> float | None:
    """Calculate the custom difference rate: log_{1.01}(new_count / old_count)"""
    if old_count <= 0 or new_count <= 0:
        return None
    return math.log(new_count / old_count) / math.log(1.01)


def get_tag_trend(
    all_df: pl.DataFrame,
    tag: str,
    new_date=None,
    old_date=None,
):
    """
    Get count and difference rate for a tag between two dates.
    If dates are None, uses the latest and previous day available.
    """
    tag_df = all_df.filter(pl.col("tagname") == tag)

    if tag_df.is_empty():
        print(f"Tag '{tag}' not found in dataset.")
        return None

    if new_date is None:
        new_date = tag_df["date"].max()
    if old_date is None:
        available_dates = tag_df["date"].sort().to_list()
        idx = available_dates.index(new_date)
        old_date = available_dates[idx - 1] if idx > 0 else None

    if old_date is None:
        print("Not enough historical data for comparison.")
        return None

    try:
        new_date = (
            parse(str(new_date)).date()
            if not isinstance(new_date, datetime.date)
            else new_date
        )
        old_date = (
            parse(str(old_date)).date()
            if not isinstance(old_date, datetime.date)
            else old_date
        )
    except Exception:
        print("Invalid date format.")
        return None

    new_count = tag_df.filter(pl.col("date") == new_date)["count"].item()
    old_count = tag_df.filter(pl.col("date") == old_date)["count"].item()

    rate = diff_rate(new_count, old_count)

    return {
        "tag": tag,
        "old_date": old_date,
        "old_count": old_count,
        "new_date": new_date,
        "new_count": new_count,
        "diff_rate": rate,
        "growth_factor": new_count / old_count if old_count > 0 else None,
    }


def top_trending_tags(
    all_df: pl.DataFrame,
    n: int = 20,
    from_date: datetime.date = None,
    to_date: datetime.date = None,
):
    """
    Find top trending tags based on the custom diff_rate over the most recent period.
    """
    if to_date is None:
        to_date = all_df["date"].max()
    if from_date is None:
        # Use the day before the latest
        available = all_df["date"].unique().sort().to_list()
        to_idx = available.index(to_date)
        from_date = available[to_idx - 1] if to_idx > 0 else to_date

    recent = all_df.filter(pl.col("date").is_in([from_date, to_date]))

    pivoted = recent.pivot(
        index="tagname", on="date", values="count", aggregate_function="first"
    ).rename({str(from_date): "old_count", str(to_date): "new_count"})

    result = pivoted.with_columns(
        pl.when(pl.col("old_count") > 0)
        .then(pl.col("new_count") / pl.col("old_count"))
        .otherwise(None)
        .log(1.01)
        .alias("diff_rate")
    ).filter(pl.col("diff_rate").is_not_null())

    return result.sort("diff_rate", descending=True).head(n)


all_df = (
    pl.scan_parquet("e_hentai_tag_counts.parquet")
    .filter(pl.col("tagname").str.starts_with("location:").not_())
    # manual fix: incorrect master tag
    .with_columns(
        pl.col("tagname")
        .replace("female:female solo", "female:sole female")
        .replace("female:animal ears", "female:kemonomimi")
    )
)

pl.Config.set_tbl_width_chars(-1)
pl.Config.set_tbl_rows(-1)
pl.Config.set_tbl_hide_dataframe_shape(True)

try:
    translation = pl.read_parquet("translation.parquet")
except Exception:
    translation = None

for threshold in [100, 500, 1000, 5000, 10000, 50000]:
    df = top_trending_tags(
        all_df.filter(pl.col("count").gt(threshold)).collect(),
        n=20,
        from_date=datetime.date(year=2025, month=12, day=1),
    )
    if translation is not None:
        df = df.join(translation, on="tagname", how="left")
        df = df.with_columns(pl.col("translation").fill_null("<无>"))
    print(df)
