from pathlib import Path

import dagster as dg
import polars as pl


@dg.asset(io_manager_key="polars_parquet_io_manager")
def additional_receipts(context: dg.AssetExecutionContext) -> pl.LazyFrame:
    """Load additional receipt data from train_checks.csv.gz.
    
    Returns
    -------
    pl.LazyFrame
        Raw additional receipt data maintaining original schema
    """
    storage_dir = Path("storage")
    train_checks_file = storage_dir / "train_checks.csv.gz"
    
    context.log.info(f"Loading additional receipts from {train_checks_file}")
    
    receipts = pl.scan_csv(train_checks_file)
    
    return receipts


@dg.asset(io_manager_key="polars_parquet_io_manager")
def additional_fiscal_data(context: dg.AssetExecutionContext) -> pl.LazyFrame:
    """Load additional fiscal data from train.csv.gz.
    
    Returns
    -------
    pl.LazyFrame
        Raw additional fiscal data maintaining original schema
    """
    storage_dir = Path("storage")
    train_file = storage_dir / "train.csv.gz"
    
    context.log.info(f"Loading additional fiscal data from {train_file}")
    
    fiscal_data = pl.scan_csv(train_file)
    
    return fiscal_data
