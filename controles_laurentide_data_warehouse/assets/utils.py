from dagster import EnvVar, AssetExecutionContext
from pyarrow import parquet as pq
from pyarrow.fs import FileSelector
from snowflake.connector.pandas_tools import write_pandas

import tempfile
import duckdb
import glob

import pandas as pd
import dask.dataframe as dd
from io import BytesIO
import io
import os

def generic_extraction_and_cleaning(context, key, s3, clean_values=True, columns_to_drop=[],
                                    dynamic_drop=False) -> pd.DataFrame:
    bucket = EnvVar("WAREHOUSE_S3_BUCKET_NAME").get_value()
    s3_path = f"{bucket}/{key}"
    file_info_list = s3.get_file_info(FileSelector(s3_path, recursive=True))

    parquet_keys = [f_info.path for f_info in file_info_list if f_info.is_file and f_info.path.endswith(".parquet")]

    dfs = []
    for parquet_key in parquet_keys:
        with s3.open_input_file(parquet_key) as f:
            df = pd.read_parquet(f)
            dfs.append(df)

    if not dfs:
        context.log.warning("No Parquet files found or read.")
        return pd.DataFrame()

    dataframes = pd.concat(dfs, ignore_index=True)

    dataframes = drop_null_columns(context, dataframes, key, columns_to_drop, dynamic_drop)
    
    for col in dataframes.select_dtypes(include=['datetimetz', 'datetime']):
        dataframes[col] = dataframes[col].dt.date
    
    return dataframes

def s3_parquet_to_dataframes(s3_client, bucket, bucket_key) -> pd.DataFrame:
    pd.set_option('display.max_columns', None)

    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=bucket_key)
    objects = response.get("Contents", [])
    parquet_keys = [obj["Key"] for obj in objects if obj["Key"].endswith(".parquet")]
    dataframes = []

    for key in parquet_keys:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        body = io.BytesIO(obj["Body"].read())
        df = pd.read_parquet(body)

        dataframes.append(df)

    if not dataframes:
        print("No Parquet files found or read.")
        return pd.DataFrame()

    combined_df = pd.concat(dataframes, ignore_index=True)
    return combined_df


def chunked_merge(
        left_df: pd.DataFrame,
        right_df: pd.DataFrame,
        on=None, left_on=None, right_on=None,
        how="inner", suffixes=("_x", "_y"), chunk_size=1_000
) -> pd.DataFrame:
    """Merge large DataFrames in chunks, dropping duplicate columns only if values match exactly."""
    chunks = []
    total = len(left_df)


    for start in range(0, total, chunk_size):
        end = start + chunk_size
        left_chunk = left_df.iloc[start:end]


        merged_chunk = pd.merge(
            left_chunk,
            right_df,
            on=on,
            left_on=left_on,
            right_on=right_on,
            how=how,
            suffixes=suffixes
        )


        common_cols = set(left_df.columns).intersection(right_df.columns)
        for col in common_cols:
            col_left = col + suffixes[0]
            col_right = col + suffixes[1]
            if col_left in merged_chunk.columns and col_right in merged_chunk.columns:
                if merged_chunk[col_left].equals(merged_chunk[col_right]):
                    merged_chunk[col] = merged_chunk[col_left]
                    merged_chunk.drop([col_left, col_right], axis=1, inplace=True)
                else:
                    pass


        chunks.append(merged_chunk)


    return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()


def sanitize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns
        .str.replace(r"[^\w]", "_", regex=True)
        .str.upper()
    )
    return df

def drop_null_columns(
    context: AssetExecutionContext,
    df: pd.DataFrame,
    table_name: str,
    columns_to_drop: list,
    dynamic_drop: bool,
) -> pd.DataFrame:
    context.log.info("Dropping columns with no values or constant non-null values in: " + table_name)
    context.log.info("Columns before dropping: " + ", ".join(df.columns))

    def is_null_or_constant(series: pd.Series) -> bool:
        first_valid_index = series.first_valid_index()

        if first_valid_index is None:
            return True

        first_value = series.loc[first_valid_index]

        return series.dropna().eq(first_value).all()

    if columns_to_drop:
        context.log.info("Static Column Drop.")

        existing_columns_to_drop = [col for col in columns_to_drop if col in df.columns]
        missing_columns = [col for col in columns_to_drop if col not in df.columns]

        if missing_columns:
            context.log.warning(
                "Columns requested to drop but not found: " + ", ".join(missing_columns)
            )

        if existing_columns_to_drop:
            df = df.drop(columns=existing_columns_to_drop)
            context.log.info("Columns dropped: " + ", ".join(existing_columns_to_drop))
        else:
            context.log.info("No matching static columns found to drop.")

    elif dynamic_drop:
        columns_to_drop = [
            col for col in df.columns
            if is_null_or_constant(df[col])
        ]

        if not columns_to_drop:
            context.log.info("No columns dropped.")
        else:
            context.log.info("Dynamic Column Drop.")
            context.log.info("Columns dropped: " + ", ".join(columns_to_drop))
            context.log.debug("Columns dropped Python List: " + str(columns_to_drop))
            df = df.drop(columns=columns_to_drop)

    else:
        context.log.info("No columns dropped.")

    context.log.info("Columns after dropping: " + ", ".join(df.columns))
    return df


def lazy_extraction_and_cleaning(context, key, s3, columns_to_drop=[]) -> dd.DataFrame:
    bucket = EnvVar("WAREHOUSE_S3_BUCKET_NAME").get_value()
    s3_path = f"{bucket}/{key}"

    file_info_list = s3.get_file_info(FileSelector(s3_path, recursive=True))
    parquet_keys = [f_info.path for f_info in file_info_list if f_info.is_file and f_info.path.endswith(".parquet")]

    if not parquet_keys:
        context.log.warning("No Parquet files found or read.")
        return dd.from_pandas(pd.DataFrame(), npartitions=1)

    temp_files = []
    for parquet_key in parquet_keys:
        with s3.open_input_file(parquet_key) as f:
            buffer = BytesIO(f.read())
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
            temp_file.write(buffer.getvalue())
            temp_file.close()
            temp_files.append(temp_file.name)

    # Now read all the temp parquet files into a single dask dataframe
    dask_df = dd.read_parquet(temp_files)

    # Drop columns if requested
    if columns_to_drop:
        dask_df = dask_df.drop(columns=columns_to_drop, errors='ignore')

    # Clean up temp files after Dask finishes reading them
    # This MUST happen later, not here, so handle cleanup after pipeline or externally

    return dask_df


def optimize_df(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.select_dtypes(include=['float']):
        df[col] = pd.to_numeric(df[col], downcast='float')
    for col in df.select_dtypes(include=['int']):
        df[col] = pd.to_numeric(df[col], downcast='integer')
    for col in df.select_dtypes(include=['object']):
        num_unique = df[col].nunique()
        num_total = len(df[col])
        if num_unique / num_total < 0.5:
            df[col] = df[col].astype('category')
    return df


def download_valid_parquet_files(
    context: AssetExecutionContext,
    s3,
    s3_prefix: str,
    local_subdir: str,
    base_temp_dir: str,
) -> list[str]:
    """
    Download *.parquet under ``s3_prefix`` into ``base_temp_dir/local_subdir``.
    Validate each file with PyArrow and return absolute paths for the valid files.
    """
    bucket = EnvVar("WAREHOUSE_S3_BUCKET_NAME").get_value()
    s3_path   = f"{bucket}/{s3_prefix}"
    files     = s3.get_file_info(FileSelector(s3_path, recursive=True))

    local_dir = os.path.join(base_temp_dir, local_subdir)
    os.makedirs(local_dir, exist_ok=True)
    good_files = []

    context.log.info(f"Scanning {s3_prefix} for Parquet files")
    for f in files:
        if not (f.is_file and f.path.endswith(".parquet")):
            continue

        try:
            with s3.open_input_file(f.path) as remote:
                buf = BytesIO()
                while chunk := remote.read(1 << 20):          # 1 MB
                    buf.write(chunk)
                if buf.tell() == 0:
                    context.log.warning(f"Zero-byte file skipped: {f.path}")
                    continue
                buf.seek(0)

                # Validate
                try:
                    pq.ParquetFile(buf)
                except Exception as e:
                    context.log.warning(f"Invalid parquet skipped: {f.path} — {e}")
                    continue

                local_path = os.path.join(local_dir, os.path.basename(f.path))
                with open(local_path, "wb") as out:
                    out.write(buf.getvalue())

                good_files.append(local_path)
        except Exception as e:
            context.log.warning(f"Failed to read {f.path}: {e}")

    if not good_files:
        context.log.warning(f"No usable Parquet files found under {s3_prefix}")
    else:
        context.log.info(f"Kept {len(good_files)} valid files under {s3_prefix}")

    return good_files

def query_parquet_with_duckdb(context, s3, parquet_map: dict[str, str], query: str):

    tmpdir = tempfile.TemporaryDirectory()
    con = duckdb.connect()
    context.log.info(f"Created temp dir for Parquet files: {tmpdir.name}")

    local_paths = {
        name: download_valid_parquet_files(context, s3, s3_prefix, name, tmpdir.name)
        for name, s3_prefix in parquet_map.items()
    }

    for alias, files in local_paths.items():
        if not files:
            raise ValueError(f"No valid Parquet files found for alias: {alias}")

    duckdb_query = query.format(**{
        name: f"{tmpdir.name}/{name}/*.parquet"
        for name in local_paths
    })

    context.log.info("Running DuckDB query from local Parquet files")
    relation = con.from_query(duckdb_query)

    return relation, con, tmpdir, local_paths

def upload_duckdb_to_snowflake(
    context,
    duckdb_relation,              # DuckDB relation from con.from_query(...)
    snowflake_resource,
    final_table_name,
):
    """
    Streams data from a DuckDB relation to Snowflake in memory-efficient chunks.
    Uses Arrow RecordBatchReader internally.
    """
    staging_table = f"{final_table_name}_staging"

    reader = duckdb_relation.to_arrow_table().to_reader()

    with snowflake_resource.get_connection() as conn:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {staging_table}")

        i = 0
        for batch in reader:
            df = batch.to_pandas()
            if df.empty:
                continue

            if i == 0:
                write_pandas(
                    conn=conn,
                    df=df,
                    table_name=staging_table,
                    auto_create_table=True,
                    quote_identifiers=False
                )
                context.log.info(f"Created and uploaded first chunk ({len(df)} rows) into {staging_table}")
            else:
                success, _, rows, *_ = write_pandas(
                    conn=conn,
                    df=df,
                    table_name=staging_table,
                    quote_identifiers=False
                )
                if not success:
                    raise RuntimeError(f"Upload failed at chunk {i}")
                context.log.info(f"Uploaded chunk {i} ({rows} rows)")

            i += 1

        if i == 0:
            context.log.warning("No data returned from DuckDB query; nothing uploaded.")
            return

        cur.execute(f"DROP TABLE IF EXISTS {final_table_name}")
        cur.execute(f"ALTER TABLE {staging_table} RENAME TO {final_table_name}")
        context.log.info(f"Replaced {final_table_name} with new data (total {i} chunks)")

def get_prefixed_columns(con, parquet_glob: str, alias: str):
    files = glob.glob(parquet_glob)
    if not files:
        raise ValueError(f"No files matched: {parquet_glob}")
    file = files[0]  # Use first real file

    # Register the file as a DuckDB view
    view_name = f"__temp_{alias}"
    con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM '{file}'")

    # Now PRAGMA works on the view name
    col_info = con.execute(f"PRAGMA table_info({view_name})").fetchall()
    columns = [row[1] for row in col_info]
    return [f"{alias}.{col} AS {col}_{alias}" for col in columns]

def prepare_local_parquet_paths(context, s3, parquet_map):
    """
    For every {alias: s3_prefix} pair, download/validate Parquet files.
    Returns (local_glob_dict, TemporaryDirectory()).
    local_glob_dict maps alias → '/tmp/.../alias/*.parquet'
    """
    tmpdir = tempfile.TemporaryDirectory()
    context.log.info(f"Created temp dir: {tmpdir.name}")

    local_paths = {}
    for alias, prefix in parquet_map.items():
        _ = download_valid_parquet_files(context, s3, prefix, alias, tmpdir.name)
        local_paths[alias] = os.path.join(tmpdir.name, alias, "*.parquet")

    return local_paths, tmpdir

def run_custom_query(context, query):
    try:    
        context.resources.snowflake_resource.execute_query(query)
        context.log.info("Table Created")
    except Exception as e:
        context.log.error("Error in Snowflake connection or SQL query")
        context.log.error("Error msg: " + str(e))
        raise

def rename_columns(df, old_cols, new_cols):
    try: 
        if len(old_cols) != len(new_cols):
            raise ValueError("old_cols and new_cols must have the same length")

        rename_map = dict(zip(old_cols, new_cols))
        return df.rename(columns=rename_map)
    except Exception as e:
        print("!!!Error: " + str(e))

def map_column_values(df, column, mapping, new_column=None):
    print("Hellow")
    print(type(df))
    print(df.columns)
    print(column)
    if new_column:
        df[new_column] = df[column].map(mapping)
    else:
        df[column] = df[column].map(mapping)
    return df


def add_surrogate_key(df, by, key_name="surrogate_key", start=1, ascending=True, within=None):
    df = df.sort_values(by=by, ascending=ascending).copy()

    if within:
        df[key_name] = (
            df.groupby(within)
              .cumcount()
              .add(start)
        )
    else:
        df[key_name] = range(start, start + len(df))

    cols = [key_name] + [c for c in df.columns if c != key_name]
    return df[cols]

def sanitize_column_names(df):
    df = df.copy()
    df.columns = (
        df.columns.astype(str)             
            .str.strip()                   
            .str.replace(r"\s+", "_", regex=True)  
            .str.replace(r"[^\w]", "", regex=True) 
            .str.lower()                    
    )
    return df

import pandas as pd
from typing import List
from dagster import AssetExecutionContext, EnvVar

SNOWFLAKE_SCHEMA = EnvVar("PROD_SNOWFLAKE_SCHEMA").get_value()
SNOWFLAKE_DATABASE = EnvVar("SNOWFLAKE_DATABASE").get_value()
SNOWFLAKE_DATABASE_TEST = EnvVar("SNOWFLAKE_DATABASE_TEST").get_value()


def pandas_dtype_to_snowflake_type(series: pd.Series) -> str:
    """
    Map a pandas Series dtype to a Snowflake column type.
    Adjust as needed for your project.
    """
    dt = series.dtype

    if pd.api.types.is_integer_dtype(dt):
        return "NUMBER(38,0)"
    if pd.api.types.is_float_dtype(dt):
        return "FLOAT"
    if pd.api.types.is_bool_dtype(dt):
        return "BOOLEAN"
    if pd.api.types.is_datetime64_any_dtype(dt):
        return "TIMESTAMP_NTZ"
    if pd.api.types.is_timedelta64_dtype(dt):
        return "TIME"
    if pd.api.types.is_categorical_dtype(dt):
        return "VARCHAR"
    return "VARCHAR"


def _get_snowflake_columns(
    context: AssetExecutionContext,
    table_name: str,
    snowflake_resource,
    database
) -> List[str]:
    """
    Return existing column names for the Snowflake table (uppercased).
    Uses global SNOWFLAKE_SCHEMA and SNOWFLAKE_DATABASE.
    """
    context.log.info(database)
    context.log.info(table_name.upper())
    context.log.info(SNOWFLAKE_SCHEMA.upper())

    try:
        result = snowflake_resource.execute_query(
            f"""
            SELECT COLUMN_NAME
            FROM {database}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{table_name.upper()}'
              AND TABLE_SCHEMA   = '{SNOWFLAKE_SCHEMA.upper()}'
            ORDER BY ORDINAL_POSITION
            """,
            fetch_results=True 
        )
        context.log.info(result)
        cols = [row[0].upper() for row in result]
    finally:
        print("Done")
    return cols


def ensure_snowflake_schema_matches_df(
    context: AssetExecutionContext,
    df: pd.DataFrame,
    snowflake_resource=None,
) -> None:
    """
    - Infers table name from the calling asset's name.
    - Uses env vars for schema and database.
    - If number of columns in Snowflake == number in df -> exit early.
    - Otherwise, adds any missing columns in Snowflake via ALTER TABLE ADD COLUMN.
    - Does NOT modify the DataFrame; asset returns it normally.
    """

    if snowflake_resource is None:
        snowflake_resource = context.resources.snowflake_resource
        database = SNOWFLAKE_DATABASE
    else: 
        database = SNOWFLAKE_DATABASE_TEST

    context.log.info(f"SNOWFLAKE RESOURCE: {snowflake_resource}")

    # Empty dataframe: nothing to sync
    if df.empty:
        context.log.info("DF is empty; skipping schema sync.")
        return

    df.columns = df.columns.str.upper()

    if context.asset_key:
        table_name = context.asset_key.path[-1].upper()
    else:
        table_name = context.op_def.name.upper()

    context.log.info(
        f"[{table_name}] Ensuring Snowflake schema matches DataFrame columns."
    )

    existing_cols = _get_snowflake_columns(context=context, table_name=table_name, snowflake_resource=snowflake_resource, database=database)
    if(len(existing_cols) == 0):
        context.log.info(f"Table {table_name} does not exist. Early Exit.")
        return

    existing_cols_set = set(existing_cols)

    if len(existing_cols) == len(df.columns):
        context.log.info(
            f"[{table_name}] Column count matches Snowflake ({len(existing_cols)}); "
            "skipping schema alteration."
        )
        return

    df_cols_set = set(df.columns)
    new_cols = sorted(df_cols_set - existing_cols_set)

    if not new_cols:
        context.log.info(
            f"[{table_name}] No new columns detected; no ALTER TABLE needed."
        )
        return

    context.log.warning(
        f"[{table_name}] Adding new columns to Snowflake: {new_cols}"
    )

    try:
        for col in new_cols:
            snowflake_type = pandas_dtype_to_snowflake_type(df[col])

            sql = (
                f'ALTER TABLE "{database.upper()}"'
                f'."{SNOWFLAKE_SCHEMA.upper()}"'
                f'."{table_name}" '
                f'ADD COLUMN "{col}" {snowflake_type}'
            )

            context.log.info(f"[{table_name}] Executing: {sql}")
            try:
                snowflake_resource.execute_query(sql)
            except Exception as e:
                msg = str(e).upper()
                if "ALREADY EXISTS" in msg:
                    context.log.warning(
                        f"[{table_name}] Column {col} already exists; ignoring."
                    )
                else:
                    raise

    finally:
        print("Done")

from datetime import datetime, timedelta

def add_shift_cron_time(cron_string, hours_to_add=0, minutes_to_add=0):
    if hours_to_add == 0 and minutes_to_add == 0:
        return cron_string

    try:
        fields = cron_string.split()
        
        if len(fields) < 5:
             raise ValueError("Cron string must have at least 5 fields (minute, hour, day_of_month, month, day_of_week).")
             
        minute_str, hour_str = fields[0], fields[1]

        current_minute = int(minute_str)
        current_hour = int(hour_str)

        base_datetime = datetime(2000, 1, 1, current_hour, current_minute)
        
        time_offset = timedelta(hours=hours_to_add, minutes=minutes_to_add)

        new_datetime = base_datetime + time_offset

        new_minute = new_datetime.minute
        new_hour = new_datetime.hour

        new_fields = [str(new_minute), str(new_hour)] + fields[2:]
        new_cron_string = " ".join(new_fields)
        
        return new_cron_string

    except ValueError as ve:
        return f"Error: {ve}"
    except Exception as e:
        return f"Error processing cron string. Ensure minute/hour are single integers: {e}"