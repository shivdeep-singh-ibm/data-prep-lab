import os
import time
from abc import ABC, abstractmethod
from functools import wraps
from typing import List, Optional

import pyarrow
import pyarrow.fs
from data_processing.utils.transform_utils import TransformUtils
from pyarrow import Table
from pyarrow.fs import FileInfo, LocalFileSystem
from pyarrow.parquet import ParquetDataset


class FileSystemCache:
    """
    Implements a simple file system cache that stores files up to a specified limit, evicts the oldest files if necessary.
    """

    def __init__(self, dir="/tmp/mycache", limit=1024 * 1024 * 1024):
        """
        Initialize the cache with a directory and a maximum size limit.

        :param dir: The base directory for storing cached files (defaults to '/tmp/mycache').
        :type dir: str
        :param limit: The maximum total size of cached files in bytes (defaults to 1GB).
        :type limit: int
        """
        # create a random temporary folder
        self.fs = LocalFileSystem()
        self.dir = dir
        self.fs.create_dir(self.dir)
        self.limit = limit
        self.used = 0
        self.file_info_list = []

    def update(self):
        """
        Update the internal list of cached files and calculate the total size used.
        """
        selector = pyarrow.fs.FileSelector(self.dir)
        file_info_list = self.fs.get_file_info(selector)
        self.file_info_list = file_info_list
        self.used = sum(list(map(lambda x: x.size, file_info_list)))

    def cached_filename(self, name):
        """
        Generate a cache filename from the given input file path.

        :param name: The input file path to generate a cache filename for.
        :type name: str
        :return: The generated cache filename.
        :rtype: str
        """
        hash_prefix = TransformUtils.str_to_int(name)
        file_name = os.path.basename(name)
        return f"{hash_prefix}-{file_name}"

    def get_internal_path_for(self, file_path):
        """
        Get the internal path for a given cache filename.

        :param file_path: The input file path to generate an internal path for.
        :type file_path: str
        :return: The generated internal path.
        :rtype: str
        """
        c_file_path = self.cached_filename(file_path)
        o_file_path = os.path.join(self.dir, c_file_path)
        return o_file_path

    def write_table_to_cache(self, file_path, table):

        o_file_path = self.get_internal_path_for(file_path)
        pyarrow.parquet.write_table(table=table, where=o_file_path, filesystem=self.fs)
        self.update()

    def read_table_from_cache(self, file_path):
        o_file_path = self.get_internal_path_for(file_path)
        table = ParquetDataset(filesystem=self.fs, path_or_paths=o_file_path).read()
        return table

    def is_present(self, file_path):
        """
        Check if a given file path is present in the cache.

        :param name: The input file path to check for presence in the cache.
        :type name: str
        :return: True if the file is present in the cache, False otherwise.
        :rtype: bool
        """
        o_file_path = self.get_internal_path_for(file_path)
        present = any(list(filter(lambda x: x.path == o_file_path, self.file_info_list)))
        if not present:
            self.update()
            present = any(list(filter(lambda x: x.path == o_file_path, self.file_info_list)))
        return present

    def evict(self, cached_files):
        """
        Evict the given list of cached files from the cache and update the internal state.

        :param cached_files: The list of cached files to be evicted.
        :type cached_files: List[str]
        """
        for file in cached_files:
            self.fs.delete_file(file)
        self.update()

    def filter_files_by_size(self, target_size):
        """
        Filter the list of cached files by size and return their paths up to the specified target size.

        :param target_size: The maximum total size in bytes for the returned filtered file list.
        :type target_size: int
        :return: A list of cache file paths that are within the target size limit.
        :rtype: List[str]
        """
        self.update()
        current_size = 0
        filtered_files = []
        sorted_file_info_list = self.file_info_list.copy()
        sorted_file_info_list.sort(key=lambda x: -x.size)  # sort descending
        for file_info in sorted_file_info_list:
            if current_size + file_info.size <= target_size:
                filtered_files.append(file_info)
                current_size += file_info.size
            else:
                break  # Stop adding more files if we exceed the target size

        return list(map(lambda x: x.path, filtered_files))

    def cache_decorator(self, func):
        @wraps(func)  # This preserves the original function's metadata
        def wrapper(*args, **kwargs):
            # Call your custom behavior before calling the original function
            # Check if the file is present.
            path = args[0]
            file_present_in_cache = self.is_present(path)
            if file_present_in_cache:
                # read from cache
                print("Successfully read from cache")
                table = self.read_table_from_cache(path)
                return table, 0
            else:
                table, retries = func(*args, **kwargs)
                # check if there is enough space
                self.apply_eviction_strategy()
                # Write the table to cache
                self.write_table_to_cache(path, table)
                # how to handle execptions of cache here
            return table, retries

        return wrapper

    def percent_full(self) -> float:
        """Calculate and return the percentage of the limit that is currently used by data."""
        return 100 * (self.used * 1.0) / self.limit

    def apply_eviction_strategy(self):
        """Apply the eviction strategy when the data structure is 98% full."""
        if self.percent_full() < 98:
            return
        else:
            # clean say 2 %
            cleanup_size = 2 / 100 * 1.0 * self.limit
            files_to_cleanup = self.filter_files_by_size(cleanup_size)
            self.evict(files_to_cleanup)
        return

    def __del__(self):
        """Release resources"""
        try:
            self.fs.delete_dir_contents(self.dir)
            self.fs.delete_dir(self.dir)
        except FileNotFoundError:
            pass


class CachedDA:
    def __init__(self):
        self.cache = FileSystemCache()

    def get_table(self, file_path):
        print(f"reading {file_path}")
        return pyarrow.Table.from_pydict({"data": [1, 2, 3]}), 0


def enforce_minimum_processing_time(process_duration):
    def decorator(func):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            elapsed_time = end_time - start_time
            if elapsed_time < process_duration:
                sleep_duration = process_duration - elapsed_time
                time.sleep(sleep_duration)
            return result

        return wrapper

    return decorator


@enforce_minimum_processing_time(5)  # Ensures the function runs for at least 5 seconds
def foo():
    print("Performing some processing...")
    # Add your actual processing here...
    time.sleep(7)  # Simulating a processing duration of 3 seconds


def test():
    fc = FileSystemCache()
    table = pyarrow.parquet.read_table("~/test.parquet")

    # test internal pathname
    internal_name = fc.get_internal_path_for("mac/test.parquet")

    file_paths = ["/myfile/av.parquet", "/myfile/aqa.parquet", "/myfile/as.parquet"]

    # Write some tables
    [fc.write_table_to_cache(file_path, table) for file_path in file_paths]
    t2 = fc.read_table_from_cache(file_paths[0])

    # check if files exist, it uses external names
    print(fc.is_present(file_paths[0]))
    print(fc.file_info_list)

    # fc.evict(['/tmp/mycache/416052091-ed.parquet'])
    print(fc.file_info_list)
    print(f"Files  {len(fc.file_info_list)}")

    # evict files of size 17320135
    # these files have internal names
    files = fc.filter_files_by_size(17320135 * 1)
    print(f"Files to evict {len(files)}")
    fc.evict(files)
    print(f"Files left: {len(fc.file_info_list)}")

    da = CachedDA()
    da.get_table("/myfile/wa.parquet")
    da.get_table = fc.cache_decorator(da.get_table)
    da.get_table("/myfile/asrti.parquet")


# test()
