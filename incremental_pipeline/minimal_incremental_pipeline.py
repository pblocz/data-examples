import polars as pl
from deltalake import DeltaTable
from typing import List

def read_watermark(table_path, watermark_table):
    watermark_df = pl.read_delta(watermark_table).filter(pl.col("table_path") == table_path)
    watermark = watermark_df.select("watermark_version").item() if watermark_df.select(pl.len()).item() != 0 else None

    return watermark

def delta_changes_from_version(
        path: str, 
        key_columns: List[str],
        starting_version: int = 0,
        ending_version: int = None,
        drop_cdf_columns: bool = True
) -> pl.DataFrame:

    dt = DeltaTable(path)
    dttable = dt.load_cdf(
        starting_version=starting_version,
        ending_version=ending_version,
    ).read_all()
    pt = pl.from_arrow(dttable)

    if starting_version: 
        pt = pt.filter(pl.col("_commit_version") > starting_version)

    # Get only the latest state for each row
    changed_rows = (
        pt
        .with_columns(rn=pl.col("_commit_version").rank("dense", descending=True).over(key_columns))
        .filter(pl.col("rn") == 1)
        .filter(pl.col("_change_type").is_in(["insert", "update_postimage"]))
        .drop("rn")
    )
    if drop_cdf_columns:
        changed_rows = changed_rows.drop("_change_type", "_commit_version", "_commit_timestamp")

    return changed_rows

# Auxiliary function that merges data or creates it if the delta does not exist
def try_merge_delta(df, table_path, predicate, content_predicate=None):
    ...

# Parameters
source_table_path = "_output/test/incremental"
target_table_path = "_output/target"
watermark_table = "_output/watermark"
merge_columnns = ["id"]

# Create empty watermark table
empty_watermark = pl.DataFrame([], schema= pl.Schema([
    ('table_path', pl.String),
    ('merge_columns', pl.List(pl.String)),
    ('watermark_version', pl.Int64),
    ('watermark_timestamp', pl.Datetime(time_unit='us', time_zone=None))]
))

DeltaTable.create(watermark_table, schema=empty_watermark.to_arrow().schema, mode="overwrite")

# Now on each increamental run
#
# Read watermark
low_watermark = read_watermark(target_table_path)

rows = delta_changes_from_version(
	source_table_path, 
	starting_version=low_watermark or 0, 
	key_columns=merge_columnns, 
	drop_cdf_columns=False
)
new_watermark_version = rows.select(pl.max("_commit_version")).item()
new_watermark_ts = (
    rows.filter(pl.col("_commit_version") == new_watermark_version).select(pl.max("_commit_timestamp")).item()
    if new_watermark_version else None
)


# Do something with rows to write to the next layer
# ...

if new_watermark_ts is not None:
    watermark_update = pl.DataFrame(
        data=[[source_table_path, merge_columnns, new_watermark_version, new_watermark_ts]], 
        schema=["table_path", "merge_columns", "watermark_version", "watermark_timestamp"], 
        orient="row"
    )

    try_merge_delta(watermark_update, watermark_table, "source.table_path == target.table_path")
