import dagster as dg
import hdbscan
import numpy as np
import polars as pl
import umap


@dg.asset(
    deps=["items_embedded"],
    io_manager_key="polars_parquet_io_manager"
)
def items_cleaned(
    context: dg.AssetExecutionContext,
    items_embedded: pl.LazyFrame
) -> pl.LazyFrame:
    """Clean items data by replacing null item_types with cluster-based majority types.
    
    Performs outlier detection, UMAP dimensionality reduction, HDBSCAN clustering,
    and assigns item_type based on cluster majority voting for null values.
    
    Parameters
    ----------
    items_embedded : pl.LazyFrame
        Items data with embeddings from the items_embedded asset
        
    Returns
    -------
    pl.LazyFrame
        Cleaned items data with null item_types filled and outliers retained
    """
    context.log.info("Starting items cleaning process")
    
    # Load and prepare data
    original_items = items_embedded.with_row_index().collect()
    original_items = original_items.with_columns(
        pl.col('item_type').replace('Не определена', None)
    )
    
    context.log.info(f"Null counts: {original_items.null_count()}")
    
    # Outlier detection and logging
    for col in ['item_quantity', 'item_price']:
        if col in original_items.columns:
            q1 = original_items.select(pl.col(col).quantile(0.25)).item()
            q3 = original_items.select(pl.col(col).quantile(0.75)).item()
            iqr = q3 - q1
            lower_bound = q1 - 3 * iqr
            upper_bound = q3 + 3 * iqr
            
            outliers_count = original_items.filter(
                (pl.col(col) < lower_bound) | (pl.col(col) > upper_bound)
            ).height
            
            context.log.info(f"Found {outliers_count} outliers in {col}")
    
    # Prepare unique items for clustering
    items = original_items.unique('item').drop_nulls('item')
    context.log.info(f"Processing {len(items)} unique items for clustering")
    
    # Extract embeddings and labels
    X = np.stack(items['embeddings'].to_list())
    y = items['item_type'].cast(pl.Categorical).to_physical().fill_null(-1).to_numpy()
    
    context.log.info("Performing UMAP dimensionality reduction")
    # UMAP dimensionality reduction
    reducer = umap.UMAP(n_components=8, min_dist=0.0, random_state=42)
    X_reduced = reducer.fit_transform(X, y)
    
    context.log.info("Performing HDBSCAN clustering")
    # HDBSCAN clustering
    clusterer = hdbscan.HDBSCAN(min_cluster_size=50, min_samples=30)
    clusterer.fit(X_reduced)
    clusters = clusterer.labels_
    
    # Add cluster labels to items
    items = items.with_columns(cluster=pl.Series(clusters))
    
    # Calculate majority type for each cluster
    majority_type = (
        items.filter(pl.col('cluster') != -1)
        .group_by('cluster')
        .agg(majority_type=pl.col('item_type').mode().first())
    )
    
    context.log.info(f"Found majority types for {len(majority_type)} clusters")
    
    # Join majority types and fill null item_types
    items = items.join(majority_type, on='cluster', how='left')
    items = items.with_columns(
        item_type=pl.coalesce(
            pl.col('item_type'),
            pl.col('majority_type')
        )
    )
    
    # Filter items that need updating (noise points or type mismatches)
    items_to_update = items.filter(
        (pl.col('cluster') == -1) |
        (pl.col('item_type') != pl.col('majority_type'))
    ).select(['item', 'item_type'])
    
    context.log.info(f"Updating {len(items_to_update)} items with cleaned types")
    
    # Update original items and clean final dataset
    cleaned_items = (
        original_items.lazy()
        .join(items_to_update.lazy(), on='item', how='left', suffix='_updated')
        .with_columns(
            item_type=pl.coalesce(
                pl.col('item_type_updated'),
                pl.col('item_type')
            )
        )
        .drop(['item_type_updated', 'index'])
        .drop_nulls(['item', 'item_type'])
    )
    
    context.log.info("Completed items cleaning process")
    
    return cleaned_items
