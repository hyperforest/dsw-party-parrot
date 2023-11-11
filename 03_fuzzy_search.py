import os
import re
import duckdb
import numpy as np
import pandas as pd

from fuzzywuzzy import fuzz
from tqdm import tqdm


def compute_fuzz(product_name: str, product_sku: str) -> float:
    return fuzz.ratio(product_name, product_sku)


def main():
    df_name = pd.read_csv('datasets/processed/product_name.tsv', sep='\t')
    catalog = pd.read_csv('datasets/processed/product_catalog.tsv', sep='\t')

    final_res = pd.read_csv('datasets/processed/final_result.tsv', sep='\t')

    duckdb.create_function('compute_fuzz', compute_fuzz)

    final_res_cp = final_res.copy()
    batch = final_res_cp.iloc[:]

    # do cross join and compute fuzzy ratio for each pair
    # by using the function `compute_fuzzy` we defined earlier,
    # and infuse it with duckdb

    result_fuzzy = duckdb.query(
        '''
        WITH joined AS (
            SELECT
                b.product_name,
                c.product_sku,
                b.possible_brand,
                b.clean_name,
                c.clean_sku,
                c.brand
            FROM
                batch AS b
            CROSS JOIN
                catalog AS c
        )

        SELECT
            product_name,
            product_sku,
            clean_name,
            clean_sku,
            possible_brand,
            compute_fuzz(clean_name, clean_sku) AS fuzzy_ratio
        FROM
            joined
        WHERE
            possible_brand = brand
            OR possible_brand IS NULL
        QUALIFY
            ROW_NUMBER() OVER (PARTITION BY clean_name ORDER BY fuzzy_ratio DESC, clean_sku) = 1
        '''
    ).to_df()

    final_res_cp = duckdb.query(
        '''
        SELECT
            f.product_name,
            r.product_sku,
            f.clean_name,
            r.clean_sku,
            f.is_name_only_alphanum,
            f.possible_brand,
            f.lev_distance,
            r.fuzzy_ratio
        FROM
            final_res_cp AS f
        LEFT JOIN
            result_fuzzy AS r
        ON
            f.clean_name = r.clean_name
        '''
    ).to_df()

    final_res_cp.to_csv('datasets/processed/final_result_2.tsv', sep='\t', index=False)

if __name__ == '__main__':
    main()
