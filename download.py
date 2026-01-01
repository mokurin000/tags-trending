import datetime
from string import Template
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import gzip
import polars as pl

# Template for the GitHub release URL
URL_TEMPLATE = Template(
    "https://github.com/mokurin000/e-hentai-tag-count/releases/download/"
    "v${year}.${month}.${day}/tagname_count.csv.gz"
)


def download_and_decompress(
    url: str,
):
    """Download and decompress a single gzipped CSV file."""
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            return gzip.decompress(response.content)
        else:
            print(f"Failed {url} — HTTP {response.status_code}")
            return None
    except Exception as e:
        print(f"Error downloading {url}: {e}")
        return None


def main():
    # Date range: from 2025-11-07 to today (2026-01-01 as per current date)
    start_date = datetime.date(2025, 11, 7)
    end_date = datetime.date.today()  # 2026-01-01

    # Generate list of dates
    dates: list[datetime.date] = []
    current = start_date
    while current <= end_date:
        dates.append(current)
        current += datetime.timedelta(days=1)

    # Build URLs using string.Template
    urls = [
        URL_TEMPLATE.substitute(
            year=date.year,
            month=f"{date.month:02}",
            day=f"{date.day:02}",
        )
        for date in dates
    ]

    # Parallel download
    print(f"Downloading {len(dates)} files in parallel...")
    data_dict = {}
    with ThreadPoolExecutor(max_workers=3) as executor:
        future_to_date = {
            executor.submit(download_and_decompress, url): date
            for date, url in zip(dates, urls)
        }
        for future in as_completed(future_to_date):
            date = future_to_date[future]
            content = future.result()
            if content is not None:
                data_dict[date] = content

    print(f"Successfully downloaded {len(data_dict)} files.")

    # Load into Polars DataFrames
    dfs = []
    for date, binary_data in data_dict.items():
        df = pl.read_csv(
            BytesIO(binary_data),
            has_header=True,
            columns=["tagname", "count"],
            schema={"tagname": pl.Utf8, "count": pl.Int64},
        ).with_columns(pl.lit(date).cast(pl.Date).alias("date"))
        dfs.append(df)

    if not dfs:
        raise RuntimeError("No data was successfully loaded.")

    all_df: pl.DataFrame = pl.concat(dfs)
    all_df = all_df.sort(by=["tagname", "date"])
    all_df.write_parquet("e_hentai_tag_counts.parquet")


if "__main__" == __name__:
    main()
