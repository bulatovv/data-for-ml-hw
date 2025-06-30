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
    
    return receipts


@dg.asset(
    deps=["additional_receipts", "additional_fiscal_data"],
    io_manager_key="polars_parquet_io_manager"
)
def preprocessed_additional_data(
    context: dg.AssetExecutionContext,
    additional_receipts: pl.LazyFrame,
    additional_fiscal_data: pl.LazyFrame
) -> pl.LazyFrame:
    """Process and combine additional receipt data with fiscal information.
    
    Parameters
    ----------
    additional_receipts : pl.LazyFrame
        Additional receipt data from CSV
    additional_fiscal_data : pl.LazyFrame
        Additional fiscal data from CSV
        
    Returns
    -------
    pl.LazyFrame
        Combined additional receipt data with fiscal information joined
    """
    context.log.info("Processing additional receipt and fiscal data")
    
    # Join additional receipts with fiscal data
    combined_additional = additional_receipts.join(
        additional_fiscal_data, 
        on='key', 
        how='left'
    )
    
    return combined_additional


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
        Processed scraped receipt data
    preprocessed_additional_data : pl.LazyFrame
        Processed additional receipt data
        
    Returns
    -------
    pl.LazyFrame
        Unified dataset with all sources concatenated
    """
    context.log.info("Concatenating all data sources")
    
    # Ensure both datasets have compatible schemas by selecting common columns
    # and adding missing columns with null values where needed
    
    # Get column names from both datasets
    scraped_columns = preprocessed_scraped_data.columns
    additional_columns = preprocessed_additional_data.columns
    
    # Find all unique columns
    all_columns = set(scraped_columns + additional_columns)
    
    # Standardize scraped data columns
    scraped_standardized = preprocessed_scraped_data.select([
        col if col in scraped_columns else pl.lit(None).alias(col) 
        for col in sorted(all_columns)
    ])
    
    # Standardize additional data columns  
    additional_standardized = preprocessed_additional_data.select([
        col if col in additional_columns else pl.lit(None).alias(col)
        for col in sorted(all_columns)
    ])
    
    # Concatenate the standardized datasets
    unified_data = pl.concat([
        scraped_standardized,
        additional_standardized
    ], how="vertical_relaxed")
    
    context.log.info("Successfully concatenated all data sources")
    
    return unified_data


# Updated Dagster definitions including all assets
defs = dg.Definitions(
    assets=[
        preprocessed_scraped_data,
        preprocessed_additional_data,
        all_sources_concatenated,
    ],
)
