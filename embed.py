import polars as pl
from sentence_transformers import SentenceTransformer

embedder = SentenceTransformer('deepvk/USER2-small')


items = pl.read_parquet('storage/items.parquet').with_row_index().drop_nulls('item').unique('item')

embed_cols = pl.DataFrame(
    {
        'index': items['index'],
        'embeddings': embedder.encode(
            items['item'].to_list(),
            prompt_name='clustering',
            show_progress_bar=True
        )
    },
    schema={
        'index': pl.UInt64,
        'embeddings': pl.Array(pl.Float32, embedder.get_sentence_embedding_dimension())
    }
)

(
    pl.scan_parquet('storage/items.parquet')
    .with_row_index()
    .join(embed_cols.lazy(), on='index')
    .select(pl.exclude('index'))
    .sink_parquet('storage/items_embedded.parquet')
)
