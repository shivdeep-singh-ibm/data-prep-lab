import os
from typing import List

import pandas as pd
import pyarrow as pa
import ray
from data_processing.utils import get_logger
from dpk_repo_level_order.internal.cache.file_system_cache import (
    FileSystemCache,
    enforce_minimum_processing_time,
)


class GroupByRepo:
    """
    This class can read a list of parquet files and transform the rows of
    `repo_column_name` and apply the user provided mapper function to them
    and write the result to local disk or s3 bucket.

    """

    def __init__(
        self,
        repo_column_name,
        output_dir,
        logger,
        data_access,
        table_mapper=None,
        enforce_minimum_processing_time_ms=0,
    ):
        self.repo_column_name = repo_column_name
        self.output_dir = output_dir
        self.logger = get_logger(__name__)
        self.data_access = data_access
        self.enable_superrows = True
        self.table_mapper = table_mapper
        self.enforce_minimum_processing_time_ms = enforce_minimum_processing_time_ms
        if self.table_mapper is None:
            """
            table_mapper is a function of signature: func(table: pa.Table, filename: str)-> List[Tuple[pa.Table, filename]]:
            """
            self.table_mapper = self._default_mapper_func

    def _default_mapper_func(self, table, file_name):
        return [
            (table, file_name),
        ]

    def process(self, repo: str, files: List[str]):
        min_proc_time = self.enforce_minimum_processing_time_ms
        if min_proc_time > 0:
            self._process = enforce_minimum_processing_time(min_proc_time)(self._process)
        return self._process(repo, files)

    def _process(self, repo: str, files: List[str]):
        try:
            repo_table = self._read_table_for_group(self.repo_column_name, repo, files)
            if len(repo_table) == 0:
                # not processing empty table
                return

            def sanitize_path(repo_name):
                return repo_name.replace("/", "%2F")

            repo = sanitize_path(repo)
            tables = self.table_mapper(repo_table, repo)

            for out_table, filename in tables:

                self.logger.info(f"Write {filename}, tables: {len(out_table)}")
                self._write_parquet(out_table, filename)
        except Exception as e:
            self.logger.error(f"Failed processing repo: {repo}. {e}")

    def _write_parquet(self, table, repo_name):
        # since we already know the repo
        # self.output_path should have the basepath where to write
        parquet_path = os.path.join(self.output_dir, f"{repo_name}.parquet")
        self.data_access.save_table(os.path.normpath(parquet_path), table)

    def _read_table_for_group(self, grouping_column, group, files):
        """This function reads the files and filters the tables based on grouping_column value"""
        dfs = []
        for file in files:
            table, _ = self.data_access.get_table(os.path.normpath(file))
            # filtering each table is more memory efficient than
            # reading all tables and filtering later.
            filtered_table = self._filter_table_by_column(table, grouping_column, group)
            dfs.append(filtered_table.to_pandas())
        df = pd.concat(dfs)

        return pa.Table.from_pandas(df)

    def _filter_table_by_column(self, table: pa.Table, column_name: str, column_value: str) -> pa.Table:
        """
        Filters rows in a PyArrow table based on the specified column value.

        Args:
            table (pa.Table): The input PyArrow table.
            column_name (str): The name of the column to filter.
            column_value (str): The value to match in the specified column.

        Returns:
            pa.Table: A new table containing only rows where the specified column has the given value.
        """

        column_data = table.column(column_name)
        row_mask = pa.compute.equal(column_data, column_value)
        filtered_table = table.filter(row_mask)

        return filtered_table


@ray.remote(scheduling_strategy="SPREAD")
class GroupByRepoActor(GroupByRepo):
    """
    This Actor represents a proxy to the class `GroupByRepo`.

    A sample `params` dict for this actor looks like.

      params = {
         'repo_column_name': str,
         'output_dir': str,
         'mapper': A function with signature: func(table: pa.Table, filename: str)->List[Tuple[pa.Table, str]]
         'data_access_creds': A dict os s3 creds or None eg. {'access_key': <>, 'secret_key': <>, 'url': <>}
      }

    """

    def __init__(self, params: dict):
        try:
            min_proc_time = params["min_process_time_ms"] * 1.0 / 1000
            if min_proc_time > 0:
                print(f"RATE LIMITING ENABLED: min_process_time={min_proc_time}sec per repo.")
        except KeyError:
            min_proc_time = 0
        data_access = self._get_data_access(params)

        super().__init__(
            params["repo_column_name"],
            params["output_dir"],
            None,
            data_access,
            params["mapper"],
            min_proc_time,
        )

    def _get_data_access(self, params):
        # Create data_access object from factory
        data_access = params["data_access_factory"].create_data_access()
        # In case the read_table_cache_limit is set,
        # let us decorate the data_access.read_table method
        # useful in case of s3 or other cloud based data_access
        # it can download the files to a temporary folder on the
        # device node and read the file from it next time, it is read.
        self.read_table_cache_limit = params["read_table_cache_limit"]
        if self.read_table_cache_limit > 0:
            read_table_cache_dir = params["read_table_cache_dir"]
            print(f"Cache dir is {read_table_cache_dir}")
            print(f"CACHING enabled for read_table. limit={self.read_table_cache_limit}MB")
            self.fs_cache = FileSystemCache(dir=read_table_cache_dir, limit=self.read_table_cache_limit * 1024 * 1024)
            data_access.get_table = self.fs_cache.cache_decorator(data_access.get_table)
        return data_access

    def __del__(self):
        super().__del__()
        if self.read_table_cache_limit:
            del self.fs_cache
