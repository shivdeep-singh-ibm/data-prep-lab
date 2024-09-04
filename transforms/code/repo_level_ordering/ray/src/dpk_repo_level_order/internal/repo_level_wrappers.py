import logging
import os
import uuid
from typing import Callable

import pyarrow as pa
from dpk_repo_level_order.internal.check_languages import (
    get_dominant_language_repo_packing,
)
from dpk_repo_level_order.internal.sorting.semantic_ordering import (
    check_and_update_title,
    sort_by_path,
    sort_sem,
)
from func_timeout.exceptions import FunctionTimedOut


SORT_BY_PATH = "SORT_BY_PATH"
SORT_SEMANTIC = "SORT_SEMANTIC"
SORT_SEMANTIC_NORMALISED = "SORT_SEMANTIC_NORMALISED"


def semantic_sort(df, logger, title_column_name):
    return sort_sem(files_df=df, logger=logger, title_column_name=title_column_name)


def semantic_sort_normalised(df, logger, title_column_name):
    check_and_update_title(df)
    return sort_sem(df, logger, title_column_name)


def get_sorting_func(
    sorting_algo: str, title_column_name: str, logger: logging.Logger
) -> Callable[[pa.Table], pa.Table]:
    if sorting_algo == SORT_SEMANTIC:
        sort_by = semantic_sort
        logger.info("semantic sort enabled")
    elif sorting_algo == SORT_SEMANTIC_NORMALISED:
        sort_by = semantic_sort_normalised
        logger.info("normalised semantic sort enabled")
    else:
        sort_by = sort_by_path
        logger.info(f"sort by path enabled")

    def sorter(file_name, table: pa.Table) -> pa.Table:
        df = table.to_pandas()
        print(f"SD: df {file_name} size: {len(df)}")
        if len(df) < 2:
            print(f"not sorting {file_name}")
            return table
        try:
            sorted_df = sort_by(df=df, logger=logger, title_column_name=title_column_name)
            return pa.Table.from_pandas(sorted_df, preserve_index=False)
        except FunctionTimedOut as e:
            logger.error(
                f"We got an exception while sorting [{file_name}].\n Exception: {e.__class__.__name__}.\n Falling back to default sort_by_path"
            )
            sorted_df = sort_by_path(df=df, logger=logger, title_column_name=title_column_name)
            return pa.Table.from_pandas(sorted_df, preserve_index=False)
        except Exception as e:
            logger.error(
                f"We got an exception while sorting [{file_name}].\n Exception: {e.__class__.__name__}.\n Falling back to default sort_by_path"
            )
            sorted_df = sort_by_path(df=df, logger=logger, title_column_name=title_column_name)
            return pa.Table.from_pandas(sorted_df, preserve_index=False)

    return sorter


def dominant_lang_per_repo(table: pa.Table, filename: str) -> str:
    """
    This function takes a table whose rows are documents from a repo
    and determines the most used/common programming language present in the
    table. It then returns the modified filename, prepended with
    the name of detected language.

    eg:  A table from file abc.parquet, with a dominant language C, will be returned
    as `C/abc.parquet`

    """
    lang_name = get_dominant_language_repo_packing(table)
    if lang_name.lower in ["c#"]:
        lang_name.replace("#", "-sharp")
    return os.path.join(lang_name, filename)


def superrow_table(table: pa.Table, repo_column_name: str) -> pa.Table:
    """
    This function combines all the rows of a parquet table into a single row
    and returns a modified table.
    """

    def language_distribution_2(table, language_column="language", size_column="size"):
        df = table.to_pandas()
        language_sums = df.groupby(language_column)[size_column].sum()
        dominant_lang = language_sums.idxmax()
        return language_sums.to_dict(), dominant_lang, language_sums[dominant_lang]

    def lang_distribution(table, grouping_column):
        """returns the language distribution dictionary like: {'Markdown': 1, 'Tex': 1, 'unknown': 11}"""
        grouped = table.group_by(grouping_column)
        aggregated = grouped.aggregate([(grouping_column, "count")])
        lang_dist = {}
        max_key = None
        max_val = 0
        for k, v in zip(aggregated[grouping_column], aggregated[f"{grouping_column}_count"]):
            lang_dist[k.as_py()] = v.as_py()
            if v.as_py() > max_val:
                max_val = v.as_py()
                max_key = k.as_py()
        return lang_dist, max_key

    super_row = table.column("contents").to_pylist()
    repo_doc_ids = table.column("document_id").to_pylist()
    from joblib import Parallel, delayed

    def generate_distributions():
        for funcs in [lang_distribution, language_distribution_2]:
            yield delayed(funcs)(table, "language")

    d_gen = Parallel(n_jobs=2, return_as="generator")(generate_distributions())
    lang_dist, dominat_lang_by_count = next(d_gen)
    print(dominat_lang_by_count)
    # distribution_by_size, dominant_lang_by_size, dominant_language_size = language_distribution_2(table, "language")
    distribution_by_size, dominant_lang_by_size, dominant_language_size = next(d_gen)
    document_id = (str(uuid.uuid4()),)

    contents = ("".join(super_row),)
    repo_document_ids = (" ".join(repo_doc_ids),)
    names = [
        "document_id",
        "contents",
        "repo_document_ids",
        "repo_name",
        "lang_distribution",
        "distribution_by_size",
        "dominant_lang_by_size",
        "dominant_language_size",
        "dominant_language_by_count",
    ]
    try:

        new_table = pa.table(
            [
                pa.array(document_id),
                pa.array(contents),
                pa.array(repo_document_ids),
                pa.array([repo_column_name]),
                pa.array([lang_dist]),
                pa.array([distribution_by_size]),
                pa.array([dominant_lang_by_size]),
                pa.array([dominant_language_size]),
                pa.array([dominat_lang_by_count]),
            ],
            names=names,
        )
    except:
        print(f"superrows error: {repo_column_name} could not be processed. contents_length: {len(contents[0])}")
        new_table = pa.table(
            [
                pa.array([]),
                pa.array([]),
                pa.array([]),
                pa.array([]),
                pa.array([]),
                pa.array([]),
                pa.array([]),
                pa.array([]),
            ],
            names=names,
        )

    return new_table


def get_transforming_func(sorting_func=None, superrows_func=None, filename_func=None):
    def my_transform(table, file_name):
        # +sd
        # allow only these languages
        allowed_languages = ["Python", "Java"]
        programming_languages = table.column("language").to_pylist()

        # convert both lists to lowercase for case-insensitive comparison
        allowed_languages = [language.lower() for language in allowed_languages]
        programming_languages = [language.lower() for language in programming_languages]

        # check if any of the allowed languages exist in the programming languages list
        exists = any(language in programming_languages for language in allowed_languages)
        if not exists:
            return []
            # -sd
        out_table = table
        original_file_name = file_name
        if sorting_func:
            out_table = sorting_func(file_name, table)
            table_before_super = out_table
            mprefix = "multirow"
        if filename_func:
            file_name = filename_func(table, file_name)
            prefix = "lang_detected"
            file_name = os.path.join(prefix, file_name)
        # detect language by size

        # detect language by frequency
        if superrows_func:
            out_table = superrows_func(out_table, file_name)
            dominant_lang_by_size = out_table.column("dominant_lang_by_size")[0].as_py()
            second_file_name = os.path.join(str(dominant_lang_by_size), original_file_name)
            prefix = "lang_detected_by_size"
            second_file_name = os.path.join(prefix, second_file_name)

        return [
            (out_table, file_name),
            (out_table, second_file_name),
            (table_before_super, os.path.join(mprefix, file_name)),
            (table_before_super, os.path.join(mprefix, second_file_name)),
        ]

    return my_transform
