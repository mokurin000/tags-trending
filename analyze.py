import datetime

import polars as pl


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

    try:
        pivoted = recent.pivot(
            index="tagname", on="date", values="count", aggregate_function="first"
        ).rename({str(from_date): "old_count", str(to_date): "new_count"})
    except pl.exceptions.ColumnNotFoundError:
        raise

    result = pivoted.with_columns(
        pl.when(pl.col("old_count") > 0)
        .then(pl.col("new_count") / pl.col("old_count"))
        .otherwise(None)
        .sub(1)
        .mul(100)
        .round(3)
        .cast(pl.String)
        .add("%")
        .alias("diff_rate")
    ).filter(pl.col("diff_rate").is_not_null())

    return result.sort("diff_rate", descending=True).head(n)


with open("mapping.csv", "r", encoding="utf-8") as f:
    lines = f.read().strip().split("\n")
    pairs = [line.split(",") for line in lines if line]
    mapping = {left: right for left, right in pairs}

all_df = (
    pl.scan_parquet("e_hentai_tag_counts.parquet")
    .filter(pl.col("tagname").str.starts_with("location:").not_())
    .filter(pl.col("tagname").str.starts_with("other:").not_())
    # manual fix: incorrect master tag
    .with_columns(
        pl.col("tagname")
        .replace(mapping)
        # manual fix: e-hentai tag that not get fixed
        .replace("male:netori", "male:minotaur")
    )
)

pl.Config.set_tbl_width_chars(-1)
pl.Config.set_tbl_rows(-1)
pl.Config.set_tbl_hide_dataframe_shape(True)

try:
    translation = pl.read_parquet("translation.parquet")
except Exception:
    translation = None

for prefix in ["female:", "male:", "parody:", "artist"]:
    print(f"----- {prefix} -----")
    for threshold in [100, 500, 1000, 10000, 50000]:
        try:
            df = top_trending_tags(
                all_df.filter(pl.col("tagname").str.starts_with(prefix))
                .filter(pl.col("count").gt(threshold))
                .collect(),
                n=20,
                from_date=datetime.date(year=2025, month=12, day=1),
            )
        except Exception:
            break
        if translation is not None:
            df = df.join(translation, on="tagname", how="left")
            df = df.with_columns(pl.col("translation").fill_null("<无>"))
        print(df)
