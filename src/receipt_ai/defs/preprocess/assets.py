from pathlib import Path

import dagster as dg
import polars as pl


@dg.asset(
    deps=["raw_receipts", "fiscal_data"],
    io_manager_key="polars_parquet_io_manager"
)
def preprocessed_scraped_data(context: dg.AssetExecutionContext) -> pl.LazyFrame:
    """Process and combine scraped receipt data with brands and fiscal information.
    
    Returns
    -------
    pl.LazyFrame
        Combined scraped receipt data with brands and fiscal information joined
    """
    storage_dir = Path("storage")
    receipts_file = storage_dir / "receipt.jsonl.gz"
    fiscal_file = storage_dir / "fiscal_data.jsonl.gz"
    
    context.log.info("Processing scraped receipt and fiscal data")
    
    # Load raw data
    q = pl.scan_ndjson(receipts_file)
    
    # Extract receipts
    receipts = q.select('receipts').explode('receipts').unnest('receipts')
    
    # Extract brands and deduplicate
    brands = (
        q.select('brands')
        .explode('brands')
        .unnest('brands')
        .unique('id')
    )
    
    # Load fiscal data
    fiscal_data = pl.scan_ndjson(fiscal_file, infer_schema_length=1000)
    
    # Join receipts with brands
    receipts = receipts.join(brands, left_on='brandId', right_on='id')
    
    # Join with fiscal data
    receipts = receipts.join(fiscal_data, on='key')
    
    # Process items following the reference pattern
    unified_scraped = (
        receipts
        .explode('items')
        .select(
            timestamp=pl.col('createdDate').str.to_datetime(),
            brand_name=pl.col('name'),  # Brand name from joined brands
            brand_type=pl.col('description'),  # Brand description
            item=pl.col('items').struct.field('name'),
            item_price=pl.col('items').struct.field('price'),
            item_quantity=pl.col('items').struct.field('quantity').cast(pl.Float64),
            category=pl.lit(None).cast(pl.Utf8),  # No category info in scraped data
            total=pl.lit(None).cast(pl.Float64)  # No total info available at item level
        )
    )
    
    return unified_scraped


@dg.asset(
    deps=["additional_receipts", "additional_fiscal_data"],
    io_manager_key="polars_parquet_io_manager"
)
def preprocessed_additional_data(
    context: dg.AssetExecutionContext,
    additional_receipts: pl.LazyFrame,
    additional_fiscal_data: pl.LazyFrame
) -> pl.LazyFrame:
    """Process and combine additional receipt data with fiscal information, mapping to unified schema.
    
    Parameters
    ----------
    additional_receipts : pl.LazyFrame
        Additional receipt data from train_checks.csv.gz
    additional_fiscal_data : pl.LazyFrame
        Additional fiscal data from train.csv.gz
        
    Returns
    -------
    pl.LazyFrame
        Combined additional receipt data mapped to unified schema
    """
    context.log.info("Processing additional receipt and fiscal data")
    
    # Join additional receipts with fiscal data on check_id
    combined_additional = additional_receipts.join(
        additional_fiscal_data, 
        left_on='check_id',
        right_on='check_id',
        how='left'
    )
    
    # Map to unified schema
    unified_additional = combined_additional.select(
        timestamp=pl.col('datetime').str.to_datetime(),
        brand_name=pl.col('shop_name'),
        brand_type=pl.lit("Additional Source"),
        item=pl.col('name'),
        item_price=pl.col('price'),
        item_quantity=pl.col('count').cast(pl.Float64),
        category=pl.col('category'),
        total=pl.col('sum')
    )
    
    return unified_additional


@dg.asset(
    deps=["preprocessed_scraped_data", "preprocessed_additional_data"],
    io_manager_key="polars_parquet_io_manager"
)
def all_sources_concatenated(
    context: dg.AssetExecutionContext,
    preprocessed_scraped_data: pl.LazyFrame,
    preprocessed_additional_data: pl.LazyFrame
) -> pl.LazyFrame:
    """Concatenate scraped and additional data into a single unified dataset.
    
    Parameters
    ----------
    preprocessed_scraped_data : pl.LazyFrame
        Processed and mapped scraped receipt data
    preprocessed_additional_data : pl.LazyFrame
        Processed and mapped additional receipt data
        
    Returns
    -------
    pl.LazyFrame
        Unified dataset with all sources concatenated
    """
    context.log.info("Concatenating all data sources")
    
    # Concatenate both datasets (both are already in unified schema)
    unified_data = pl.concat([
        preprocessed_scraped_data,
        preprocessed_additional_data
    ], how="diagonal_relaxed").with_row_index('basket_id')
    
    context.log.info("Successfully concatenated all data sources")
    
    return unified_data
