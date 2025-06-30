from pathlib import Path

import dagster as dg
import polars as pl


@dg.asset(io_manager_key="polars_parquet_io_manager")
def additional_receipts(context: dg.AssetExecutionContext) -> pl.LazyFrame:
    """Process additional receipt data from train_checks.csv.gz.
    
    Returns
    -------
    pl.LazyFrame
        Additional receipt data with unified schema
    """
    storage_dir = Path("storage")
    train_checks_file = storage_dir / "train_checks.csv.gz"
    
    context.log.info(f"Loading additional receipts from {train_checks_file}")
    
    receipts = (
        pl.scan_csv(train_checks_file)
        .select(
            key=pl.col('check_id').cast(pl.Utf8),  # Convert to string to match scraped data
            createdDate=pl.col('datetime'),
            brandId=pl.lit(None).cast(pl.Utf8),  # No brand info in additional data
            name=pl.col('shop_name'),  # Shop name as brand name
            description=pl.lit("Additional Receipt").cast(pl.Utf8),  # Default description
            total=pl.col('sum')
        )
    )
    
    return receipts


@dg.asset(io_manager_key="polars_parquet_io_manager")
def additional_fiscal_data(context: dg.AssetExecutionContext) -> pl.LazyFrame:
    """Process additional fiscal data from train.csv.gz.
    
    Returns
    -------
    pl.LazyFrame
        Additional fiscal data with individual items
    """
    storage_dir = Path("storage")
    train_file = storage_dir / "train.csv.gz"
    
    context.log.info(f"Loading additional fiscal data from {train_file}")
    
    fiscal_data = (
        pl.scan_csv(train_file)
        .select(
            key=pl.col('check_id').cast(pl.Utf8),  # Convert to string to match scraped data
            items=pl.struct([
                pl.col('name'),
                pl.col('price'),
                pl.col('count').alias('quantity')
            ])
        )
        .group_by('key')
        .agg(
            items=pl.col('items')
        )
    )
    
    return fiscal_data
