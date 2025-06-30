import asyncio
import gzip
import json
from pathlib import Path

import dagster as dg
import polars as pl
from dagster_polars import PolarsParquetIOManager
from pydoll.browser.tab import Tab

from .resources import ApiResource


@dg.asset
async def raw_receipts(
    context: dg.AssetExecutionContext,
    tab: dg.ResourceParam[Tab],
    api: ApiResource
): 
    """Scrape raw receipt data from the API.
    
    Returns
    -------
    dg.MaterializeResult
        Materialization result with metadata about scraped receipts
    """
    storage_dir = Path("storage")
    storage_dir.mkdir(exist_ok=True)
    receipts_file = storage_dir / "receipt.jsonl.gz"
    
    # Remove existing file to start fresh
    if receipts_file.exists():
        receipts_file.unlink()
    
    total_receipts = 0
    page_num = 0
    has_more = True
    
    while has_more:
        context.log.info(f"Fetching receipts page {page_num}")
        
        data = await api.fetch_receipts_page(tab, page_num)
        
        with gzip.open(receipts_file, 'at', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False)
            f.write('\n')
        
        receipts_count = len(data.get('receipts', []))
        total_receipts += receipts_count
        has_more = data.get('hasMore', False)
        page_num += 1
        
        context.log.info(f"Scraped {receipts_count} receipts from page {page_num - 1}")
        await asyncio.sleep(3)  # Rate limiting
    
    return dg.MaterializeResult(
        metadata={
            "total_receipts": total_receipts,
            "total_pages": page_num,
            "file_path": str(receipts_file)
        }
    )


@dg.asset(deps=["raw_receipts"],)
async def fiscal_data(
    context: dg.AssetExecutionContext,
    tab: dg.ResourceParam[Tab],
    api: dg.ResourceParam[ApiResource]
):
    """Fetch detailed fiscal data for each receipt.
    
    Returns
    -------
    dg.MaterializeResult
        Materialization result with metadata about fiscal data
    """
    storage_dir = Path("storage")
    receipts_file = storage_dir / "receipt.jsonl.gz"
    fiscal_file = storage_dir / "fiscal_data.jsonl.gz"
    
    if not receipts_file.exists():
        raise dg.DagsterInvariantViolationError("Receipts file not found")
    
    # Load existing fiscal data to avoid duplicates
    if fiscal_file.exists():
        existing_keys = set(
            pl.scan_ndjson(fiscal_file)
            .select('key')
            .collect()['key']
            .to_list()
        )
    else:
        existing_keys = set()
    
    # Get all receipt keys that need fiscal data
    receipt_keys = (
        pl.scan_ndjson(receipts_file)
        .explode('receipts')
        .unnest('receipts')
        .select('key')
        .collect()['key']
        .to_list()
    )
    
    keys_to_fetch = [key for key in receipt_keys if key not in existing_keys]
    context.log.info(f"Need to fetch fiscal data for {len(keys_to_fetch)} receipts")
    
    if not keys_to_fetch:
        return dg.MaterializeResult(
            metadata={"fiscal_records_fetched": 0, "message": "No new keys to fetch"}
        )
    
    fetched_count = 0
    for i, key in enumerate(keys_to_fetch):
        context.log.info(f"Fetching fiscal data {i+1}/{len(keys_to_fetch)} for key: {key}")
        
        try:
            data = await api.fetch_fiscal_data(tab, key)
            
            with gzip.open(fiscal_file, 'at', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False)
                f.write('\n')
            
            fetched_count += 1
            await asyncio.sleep(3)  # Rate limiting
            
        except Exception as e:
            context.log.error(f"Failed to fetch fiscal data for key {key}: {e}")
            continue
    
    return dg.MaterializeResult(
        metadata={
            "fiscal_records_fetched": fetched_count,
            "total_keys_processed": len(keys_to_fetch),
            "file_path": str(fiscal_file)
        }
    )


@dg.asset(
    deps=["raw_receipts", "fiscal_data"],
    io_manager_key="polars_parquet_io_manager"
)
def processed_receipts(context: dg.AssetExecutionContext) -> pl.LazyFrame:
    """Process and combine receipt data with fiscal information.
    
    Returns
    -------
    pl.LazyFrame
        Combined and processed receipt data as LazyFrame
    """
    storage_dir = Path("storage")
    receipts_file = storage_dir / "receipt.jsonl.gz"
    fiscal_file = storage_dir / "fiscal_data.jsonl.gz"
    
    # Load and process receipts
    receipts_df = (
        pl.scan_ndjson(receipts_file, infer_schema_length=10000)
        .explode('receipts')
        .unnest('receipts')
    )
    
    # Load fiscal data if available
    if fiscal_file.exists():
        fiscal_df = pl.scan_ndjson(fiscal_file)
        
        # Join receipts with fiscal data
        combined_df = receipts_df.join(fiscal_df, on='key', how='left')
    else:
        combined_df = receipts_df
    
    return combined_df


@dg.asset(io_manager_key="polars_parquet_io_manager")
def receipt_summary(processed_receipts: pl.LazyFrame) -> pl.LazyFrame:
    """Create a summary of receipt data.
    
    Parameters
    ----------
    processed_receipts : pl.LazyFrame
        Input LazyFrame from processed_receipts asset
        
    Returns
    -------
    pl.LazyFrame
        Summary statistics of receipts
    """
    return (
        processed_receipts
        .group_by("date")
        .agg([
            pl.count().alias("receipt_count"),
            pl.col("amount").sum().alias("total_amount"),
            pl.col("amount").mean().alias("avg_amount")
        ])
        .sort("date")
    )


@dg.asset(io_manager_key="polars_parquet_io_manager")
def receipt_items(processed_receipts: pl.LazyFrame) -> pl.LazyFrame | None:
    """Extract individual items from receipts.
    
    Parameters
    ----------
    processed_receipts : pl.LazyFrame
        Input LazyFrame from processed_receipts asset
        
    Returns
    -------
    pl.LazyFrame | None
        Individual receipt items, or None if no items found
    """
    items_df = (
        processed_receipts
        .filter(pl.col("items").is_not_null())
        .explode("items")
        .unnest("items")
    )
    
    # Return None if no items found
    item_count = items_df.select(pl.len()).collect().item()
    if item_count == 0:
        return None
        
    return items_df


# Updated Dagster definitions
defs = dg.Definitions(
    assets=[
        raw_receipts, 
        fiscal_data, 
        processed_receipts, 
        receipt_summary, 
        receipt_items,
    ],
    resources={
        "polars_parquet_io_manager": PolarsParquetIOManager(base_dir="storage"),
    }
)
