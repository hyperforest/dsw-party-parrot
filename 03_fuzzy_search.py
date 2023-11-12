import duckdb
import pandas as pd
from time import time
from fuzzywuzzy import fuzz


def compute_fuzz(product_name: str, product_sku: str) -> float:
    return fuzz.ratio(product_name, product_sku)


def count_common_tokens(product_name: str, product_sku: str) -> int:
    name = set(product_name.split(" "))
    sku = set(product_sku.split(" "))

    return len(name & sku)


def timeit(func):
    def wrapper(*args, **kwargs):
        start = time()
        result = func(*args, **kwargs)
        print(f"Time elapsed for {func.__name__}: {time() - start:.2f} seconds")
        return result

    return wrapper


def is_name_token_present_in_sku(name, sku) -> bool:
    if name is None or sku is None:
        return False

    name_tokens = name.split()
    sku_tokens = sku.split()
    for token in name_tokens:
        if (len(token) <= 2 and token != 'za') or token in ['plus']:
            continue
        if token in sku_tokens:
            return True
    return False


@timeit
def main():
    df_name = pd.read_csv("datasets/processed/product_name.tsv", sep="\t")  # noqa: F841
    catalog = pd.read_csv("datasets/processed/product_catalog.tsv", sep="\t")  # noqa: F841
    result_lev = pd.read_csv("datasets/processed/result_lev.tsv", sep="\t")  # noqa: F841

    duckdb.create_function("compute_fuzz", compute_fuzz)
    duckdb.create_function("count_common_tokens", count_common_tokens)
    duckdb.create_function("is_name_token_present_in_sku", is_name_token_present_in_sku)

    result_fuzzy = duckdb.query(  # noqa: F841
        """
        WITH joined AS (
            SELECT
                dn.*,
                c.*,
                is_name_token_present_in_sku(
                    dn.clean_name_non_formula,
                    c.clean_sku || ' ' || c.brand
                ) AS is_name_token_present_in_sku_or_brand,
                compute_fuzz(dn.clean_name, c.clean_sku) AS fuzzy_ratio,
            FROM
                df_name AS dn
            CROSS JOIN
                catalog AS c
        )

        SELECT
            *,
            levenshtein(clean_name_non_formula, clean_sku) AS lev_dist_fuzzy_wo_form
        FROM
            joined
        WHERE
            (possible_brand IS NOT NULL AND possible_brand = brand)
            OR (possible_brand IS NOT NULL AND possible_brand != brand AND fuzzy_ratio >= 70)
            OR possible_brand IS NULL
        QUALIFY
            ROW_NUMBER() OVER (
                PARTITION BY
                    product_id
                ORDER BY
                    possible_brand IS NOT NULL AND possible_brand = brand DESC,
                    possible_brand IS NOT NULL DESC,
                    is_name_token_present_in_sku_or_brand DESC,
                    fuzzy_ratio DESC,
                    lev_dist_fuzzy_wo_form,
                    clean_sku,
                    product_id
            ) = 1
        """
    ).to_df()

    result_fuzzy = duckdb.query(
        """
        SELECT
            rl.product_id,
            rl.product_name,
            rl.result_sku_lev,
            rf.product_sku AS result_sku_fuzzy,
            rl.result_sku_id_lev,
            rf.sku_id AS result_sku_id_fuzzy,

            rl.possible_brand,
            rl.is_name_only_alphanum,
            rl.is_name_only_alphabet,
            rl.clean_name_non_formula,
            rl.clean_name_formula,

            rl.clean_name,
            rl.result_clean_sku_lev,
            rf.clean_sku AS result_clean_sku_fuzzy,

            rf.fuzzy_ratio,
            
            rl.lev_dist_lev,
            levenshtein(rf.clean_name, rf.clean_sku) AS lev_dist_fuzzy,
            
            rl.lev_dist_lev_wo_form,
            rf.lev_dist_fuzzy_wo_form,
            
            is_name_token_present_in_sku(rl.clean_name, rl.result_clean_sku_lev) AS is_name_token_present_in_sku_lev,
            is_name_token_present_in_sku(rf.clean_name, rf.clean_sku) AS is_name_token_present_in_sku_fuzzy,

            rl.cnt_common_tokens_lev,
            count_common_tokens(rf.clean_name, rf.clean_sku) AS cnt_common_tokens_fuzzy,
        FROM
            result_lev AS rl
        LEFT JOIN
            result_fuzzy AS rf
        ON
            rl.product_id = rf.product_id
        ORDER BY
            rl.product_id
        """
    ).to_df()

    result_fuzzy.to_csv("datasets/processed/result_fuzzy.tsv", sep="\t", index=False)


if __name__ == "__main__":
    main()
