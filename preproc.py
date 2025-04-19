import polars as pl

q = pl.scan_ndjson('storage/receipt.jsonl.gz')

receipts = q.select('receipts').explode('receipts').unnest('receipts')
brands = (
    q.select('brands')
    .explode('brands').unnest('brands')
    .unique('id')
)
fiscal_data = pl.scan_ndjson('storage/fiscal_data.jsonl.gz', infer_schema_length=1000)

receipts = receipts.join(brands, left_on='brandId', right_on='id')
receipts = receipts.join(fiscal_data, on='key')


items_annotated = pl.scan_csv('storage/train.csv').select(
    basket_id='check_id',
    item='name',
    item_type='category',
    item_price='price',
    item_quantity='count'
)

items = (
    receipts
    .with_row_index('basket_id', offset=items_annotated.count().collect().item(0, 0) + 1)
    .explode('items')
    .select(
        'basket_id',
        timestamp=pl.col('createdDate').str.to_datetime(),
        brand_name='name',
        brand_type='description',
        item=pl.col('items').struct.field('name'),
        item_price=pl.col('items').struct.field('price'),
        item_quantity=pl.col('items').struct.field('quantity').cast(pl.Float64)
    )
)

pl.concat([items, items_annotated], how='diagonal_relaxed').sink_parquet('storage/items.parquet')
