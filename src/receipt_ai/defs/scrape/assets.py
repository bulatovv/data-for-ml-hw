import asyncio
import gzip
import json
from pathlib import Path

import dagster as dg
import polars as pl
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
    
    receipt_keys = (
        pl.scan_ndjson(receipts_file)
        .explode('receipts')
        .unnest('receipts')
        .select('key')
        .collect()['key']
        .to_list()
    )
    
    context.log.info(f"Need to fetch fiscal data for {len(receipt_keys)} receipts")
    
    if not receipt_keys:
        return dg.MaterializeResult(
            metadata={"fiscal_records_fetched": 0, "message": "No new keys to fetch"}
        )
    
    fetched_count = 0
    for i, key in enumerate(receipt_keys):
        context.log.info(f"Fetching fiscal data {i+1}/{len(receipt_keys)} for key: {key}")
        
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
            "total_keys_processed": len(receipt_keys),
            "file_path": str(fiscal_file)
        }
    )


# Updated Dagster definitions
defs = dg.Definitions(
    assets=[
        raw_receipts, 
        fiscal_data, 
    ],
)
