import polars as pl

pl.Config.set_tbl_cols(10)
pl.Config.set_tbl_rows(25)
df = pl.read_parquet('storage/items.parquet')

print(df.describe())
df.glimpse()

print(df['brand_name'].drop_nulls().value_counts().sort('count', descending=True))
print(df['brand_type'].drop_nulls().value_counts().sort('count', descending=True))
print(df['item_type'].drop_nulls().value_counts().sort('count', descending=True))
