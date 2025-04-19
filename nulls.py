import polars as pl

items = pl.read_parquet('storage/items.parquet')

print(items.null_count())
