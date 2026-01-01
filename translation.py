import orjson
import polars as pl

schema = {"tagname": pl.String, "translation": pl.String}
df = pl.DataFrame(schema=schema)

with open("db.full.json", "rb") as f:
    data: dict[str, dict] = orjson.loads(f.read())

for entry in data["data"]:
    namespace = entry["namespace"]
    entry_data: dict[str, dict] = entry["data"]

    new_df = pl.from_dicts(
        {
            "tagname": f"{namespace}:{tag}",
            "translation": taginfo["name"]["text"],
        }
        for tag, taginfo in entry_data.items()
    )
    df.vstack(new_df, in_place=True)

df.write_parquet("translation.parquet")