import re
import json
import numpy as np
import pandas as pd

from time import time


def is_containing_non_alphanumeric(string):
    return not bool(re.search(r"[^a-zA-Z0-9 ]", string))


def is_alphabet_only(string):
    return not bool(re.search(r"[^a-zA-Z ]", string))


def clean(s):
    s = s.lower()

    # give spaces before and after the special characters, that are not number or decimal number
    # e.g. 'abc+c' to 'abc + c', but '12.9' not to '12 . 9'
    s = re.sub(r"([\@\-\"\'])", r" \1 ", s)

    # remove parentheses
    s = re.sub(r"(\(|\))", r"", s)

    # set non alphabets, non numbers, and non punctuations to give spaces sign
    s = re.sub(r"([^a-zA-Z0-9\.\,\+\-\ ]+)", r" \1 ", s)

    # give another spaces
    s = re.sub(r"(\+|,)", r" \1 ", s)
    s = re.sub(r"([a-z]+)(\.)", r"\1 \2", s)
    s = re.sub(r"([a-zA-Z]+)([0-9]+)", r"\1 \2", s)

    # separate value and units like "100ml" to "100 ml" or "50g" to "50 g"
    # that are not wrapped in <...> bracket
    s = re.sub(r"(\d+)([a-zA-Z]+)", r"\1 \2", s)

    # make chemical formula separator uniform to 'x', i.e. '4.5 - 3.6 - 2.1' to '4.5x3.6x2.1'
    s = re.sub(
        r"(\d+\.?,?\d*)[×x\+,\. |-]+(\d+\.?,?\d*)[×x\+,\. |-]+(\d+\.?,?\d*)",
        r"\1x\2x\3",
        s,
    )

    # separate spaces from chemical formula
    s = re.sub(r"(\d+)(\+|-)", r"\1 +", s)
    s = re.sub(r"(\+|-)(\d+) |\+([a-zA-Z]+)", r"\1 \2 \3", s)
    s = re.sub(r"([a-zA-Z]+)(\+|-)", r"\1 \2", s)
    s = re.sub(r"(\+|-)([0-9]+)", r"\1 \2", s)

    # separate comma from alphabets
    s = re.sub(r"([a-zA-Z]+),([a-zA-Z]+)", r"\1 , \2", s)

    s = re.sub(r"\s+", " ", s)  # remove extra spaces once again

    return s


def extract_non_formula(text):
    pattern = r"^[^\d]+"
    match = re.search(pattern, text)

    if match:
        s = match.group()

        # remove characters other than alphabets, numbers, and spaces
        s = re.sub(r"[^a-zA-Z0-9 ]", "", s)

        return s.strip()

    return np.nan


def extract_formula(s):
    match = re.search(r"\d+\.?,?\d*x\d+\.?,?\d*x+\d+\.?,?\d*", s)
    if match:
        return match.group()
    return np.nan


def extract_alpha_num_only(s):
    return re.sub(r"[^a-zA-Z0-9 ]", "", s)


def timeit(func):
    def wrapper(*args, **kwargs):
        start = time()
        res = func(*args, **kwargs)
        end = time()
        print(f"Time taken: {end - start:.2f} seconds")
        return res

    return wrapper


@timeit
def main():
    df_name = pd.read_excel("datasets/raw/product_name.xlsx")
    catalog = pd.read_excel("datasets/raw/product_catalog.xlsx")

    df_name = df_name.reset_index().rename(columns={"index": "product_id"})
    catalog = catalog.reset_index().rename(columns={"index": "sku_id"})

    df_name.rename(columns={"Product Name": "product_name"}, inplace=True)
    catalog.rename(
        columns={
            "Product SKU": "product_sku",
            "Brand": "brand",
            "Type": "type",
            "Formula": "formula",
        },
        inplace=True,
    )

    df_name = df_name.dropna()

    df_name["is_name_only_alphanum"] = df_name["product_name"].apply(
        is_containing_non_alphanumeric
    )
    df_name["is_name_only_alphabet"] = df_name["product_name"].apply(is_alphabet_only)
    df_name["clean_name"] = df_name.product_name.apply(clean)
    df_name["clean_name_alphanum"] = df_name.clean_name.apply(extract_alpha_num_only)
    df_name["clean_name_non_formula"] = df_name.clean_name.apply(extract_non_formula)
    df_name["clean_name_formula"] = df_name.clean_name.apply(extract_formula)

    catalog["is_sku_only_alphanum"] = catalog["product_sku"].apply(
        is_containing_non_alphanumeric
    )
    catalog["is_sku_only_alphabet"] = catalog["product_sku"].apply(is_alphabet_only)
    catalog["clean_sku"] = catalog.product_sku.apply(clean)
    catalog["clean_sku_alphanum"] = catalog.clean_sku.apply(extract_alpha_num_only)
    catalog["clean_sku_non_formula"] = catalog.clean_sku.apply(extract_non_formula)
    catalog["clean_sku_formula"] = catalog.clean_sku.apply(extract_formula)
    
    catalog["brand"] = catalog.brand.str.lower().str.replace("/", " ")
    catalog["type"] = catalog.type.str.lower()

    # tag possible brand
    unique_brand = sorted(catalog.brand.unique())
    with open("common_tokens.json", "r", encoding='utf-8') as f:
        brand_sku_tokens = json.load(f)

    df_name['possible_brand'] = None 
    for brand in unique_brand:
        pattern = brand_sku_tokens[brand]
        df_name.loc[
            df_name.clean_name.str.replace(" ", "").str.contains(pattern),
            'possible_brand'
        ] = brand

    df_name.to_csv("datasets/processed/product_name.tsv", sep="\t", index=False)
    catalog.to_csv("datasets/processed/product_catalog.tsv", sep="\t", index=False)


if __name__ == "__main__":
    main()
