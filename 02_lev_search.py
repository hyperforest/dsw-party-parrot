import os
import re
import duckdb
import numpy as np
import pandas as pd

from fuzzywuzzy import fuzz
from tqdm import tqdm


def main():
    df_name = pd.read_csv('datasets/processed/product_name.tsv', sep='\t')
    catalog = pd.read_csv('datasets/processed/product_catalog.tsv', sep='\t')

    catalog.brand = catalog.brand.str.lower().str.replace(' ', '')

    joined = duckdb.query(
        '''
        WITH joined AS (
            SELECT
                dn.clean_name,
                dn.possible_brand,
                c.product_sku,
                c.clean_sku
            FROM
                df_name AS dn
            CROSS JOIN
                catalog AS c
        )

        SELECT
            clean_name,
            product_sku,
            clean_sku,
            possible_brand,
            levenshtein(clean_name, clean_sku) AS lev_distance
        FROM
            joined
        '''
    ).to_df()

    result_lev = duckdb.query(
        """
        SELECT
            *
        FROM
            joined
        WHERE
            lev_distance < 2
        QUALIFY
            ROW_NUMBER() OVER (PARTITION BY clean_name ORDER BY lev_distance, clean_sku) = 1
        """
    ).to_df()

    final_res = df_name.copy()

    final_res = duckdb.query(
        '''
        SELECT
            f.product_name,
            r.product_sku,
            f.clean_name,
            r.clean_sku,
            f.is_only_alphanum AS is_name_only_alphanum,
            f.possible_brand,
            r.lev_distance,
        FROM
            final_res AS f
        LEFT JOIN
            result_lev AS r
        ON
            f.clean_name = r.clean_name
        '''
    ).to_df()

    final_res.to_csv('datasets/processed/final_result.tsv', sep='\t', index=False)


if __name__ == '__main__':
    main()
