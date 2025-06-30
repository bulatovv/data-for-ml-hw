import dagster as dg
import polars as pl

from .resources import EmbedderResource


@dg.asset(
    deps=["all_sources_concatenated"],
    io_manager_key="polars_parquet_io_manager"
)
def items_embedded(
    context: dg.AssetExecutionContext,
    embedder: dg.ResourceParam[EmbedderResource],
    all_sources_concatenated: pl.LazyFrame
) -> pl.LazyFrame:
    """Generate embeddings for unique items from all sources data.
    
    Parameters
    ----------
    embedder : EmbedderResource
        The sentence transformer embedder resource
    all_sources_concatenated : pl.LazyFrame
        Unified dataset with all sources concatenated
        
    Returns
    -------
    pl.LazyFrame
        Items data with embeddings added
    """
    context.log.info("Starting item embedding process")
    
    # Load items data and prepare for embedding
    items = (
        all_sources_concatenated
        .with_row_index()
        .drop_nulls('item')
        .unique('item')
        .collect()
    )
    
    context.log.info(f"Found {len(items)} unique items to embed")
    
    # Generate embeddings
    item_texts = items['item'].to_list()
    embeddings = embedder.encode(
        item_texts,
        prompt_name='clustering',
        show_progress_bar=True
    )
    
    # Create embeddings dataframe
    embed_cols = pl.DataFrame(
        {
            'index': items['index'],
            'embeddings': embeddings
        },
        schema={
            'index': pl.UInt64,
            'embeddings': pl.Array(
                pl.Float32, 
                embedder.get_sentence_embedding_dimension()
            )
        }
    )
    
    context.log.info("Generated embeddings successfully")
    
    # Join embeddings back with original items data
    items_with_embeddings = (
        all_sources_concatenated
        .with_row_index()
        .join(embed_cols.lazy(), on='index')
        .select(pl.exclude('index'))
    )
    
    context.log.info("Completed item embedding process")
    
    return items_with_embeddings


defs = dg.Definitions(
    assets=[items_embedded],
    resources={
        'embedder': EmbedderResource(model_name='deepvk/USER2-small')
    }
)
