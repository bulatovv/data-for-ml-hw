import dagster as dg
from dagster_polars import PolarsParquetIOManager

defs = dg.Definitions(
    resources={
        "polars_parquet_io_manager": PolarsParquetIOManager(base_dir="storage"),
    }
)
