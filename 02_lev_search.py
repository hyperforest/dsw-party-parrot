import duckdb
import pandas as pd

from time import time


def timeit(func):
    def wrapper(*args, **kwargs):
        start = time()
        result = func(*args, **kwargs)
        print(f"Time elapsed: {time() - start:.2f} seconds")
        return result

    return wrapper


def count_common_tokens(product_name: str, product_sku: str) -> int:
    name = set(product_name.split(" "))
    sku = set(product_sku.split(" "))

    return len(name & sku)


@timeit
def main():
    df_name = pd.read_csv("datasets/processed/product_name.tsv", sep="\t") # noqa: F841
    catalog = pd.read_csv("datasets/processed/product_catalog.tsv", sep="\t")
    catalog.brand = catalog.brand.str.lower().str.replace(" ", "")

    duckdb.create_function("count_common_tokens", count_common_tokens)

    result_lev = duckdb.query(  # noqa: F841
        """
        WITH joined AS (
            SELECT
                dn.*,
                c.*
            FROM
                df_name AS dn
            CROSS JOIN
                catalog AS c
        )

        , final AS (
            SELECT
                dn.product_id,
                dn.product_name,
                j.product_sku AS result_sku_lev,
                j.sku_id AS result_sku_id_lev,
                j.possible_brand,
                j.is_name_only_alphanum,
                j.is_name_only_alphabet,
                j.clean_name_non_formula,
                j.clean_name_formula,
                j.clean_name,
                j.clean_sku AS result_clean_sku_lev,
                levenshtein(j.clean_name, j.clean_sku) AS lev_dist_lev,
                levenshtein(j.clean_name_non_formula, j.clean_sku) AS lev_dist_lev_wo_form,
                count_common_tokens(j.clean_name, j.clean_sku) AS cnt_common_tokens_lev
            FROM
                df_name AS dn
            LEFT JOIN
                joined AS j
            ON
                dn.clean_name = j.clean_name
            QUALIFY
                ROW_NUMBER() OVER (
                    PARTITION BY
                        dn.product_id
                    ORDER BY
                        lev_dist_lev,
                        lev_dist_lev_wo_form,
                        j.clean_sku,
                        dn.product_id
                ) = 1
        )

        SELECT
            *
        FROM
            final
        ORDER BY
            product_id
        """
    ).to_df()

    result_lev.to_csv("datasets/processed/result_lev.tsv", sep="\t", index=False)


if __name__ == "__main__":
    main()
