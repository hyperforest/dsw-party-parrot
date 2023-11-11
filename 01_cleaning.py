import os
import re
import duckdb
import numpy as np
import pandas as pd

from fuzzywuzzy import fuzz
from tqdm import tqdm


def is_containing_non_alphanumeric(string):
    return not bool(re.search(r'[^a-zA-Z0-9 ]', string))


def clean(s):
    s = s.lower()

    # give spaces before and after the special characters, that are not number or decimal number
    # e.g. 'abc+c' to 'abc + c', but '12.9' not to '12 . 9'
    s = re.sub(r'([^\d\.,-]+)', r' \1 ', s)

    # separate value and units like "100ml" to "100 ml" or "50g" to "50 g"
    # that are not wrapped in <...> bracket
    s = re.sub(r'(\d+)([a-zA-Z]+)', r'\1 \2', s)

    # make chemical formula separator uniform to 'x', i.e. '4.5 - 3.6 - 2.1' to '4.5x3.6x2.1'
    s = re.sub(r'(\d+\.?,?\d*)[×x\+,\. |-]+(\d+\.?,?\d*)[×x\+,\. |-]+(\d+\.?,?\d*)', r'\1x\2x\3', s)

    # remove extra spaces
    s = re.sub(r'\s+', ' ', s)
    
    return s


def main():
    df_name = pd.read_excel('datasets/raw/product_name.xlsx')
    catalog = pd.read_excel('datasets/raw/product_catalog.xlsx')

    df_name.rename(columns={'Product Name': 'product_name'}, inplace=True)
    catalog.rename(columns={
        'Product SKU': 'product_sku',
        'Brand': 'brand',
        'Type': 'type',
        'Formula': 'formula'
    }, inplace=True)

    df_name = df_name.dropna()

    df_name['is_only_alphanum'] = df_name['product_name'].apply(is_containing_non_alphanumeric)
    catalog['is_only_alphanum'] = catalog['product_sku'].apply(is_containing_non_alphanumeric)

    catalog['clean_sku'] = catalog.product_sku.apply(clean)
    df_name['clean_name'] = df_name.product_name.apply(clean)
    catalog['brand'] = catalog.brand.str.lower().str.replace(' ', '')

    # tag possible brand
    unique_brand = sorted(catalog.brand.unique())
    brand_sku_tokens = {}

    for brand in unique_brand:
        brand_sku_tokens[brand] = set()

        for sku in catalog[catalog.brand == brand].clean_sku:
            brand_sku_tokens[brand] |= set(sku.split(' '))

    for brand in brand_sku_tokens:
        # keep alphabetics with length >= 4 only
        brand_sku_tokens[brand] = set(filter(lambda x: len(x) >= 4 and x.isalpha(), brand_sku_tokens[brand]))

    brand_sku_tokens['dgw/hextar'] |= {'hx', 'dgw'}
    # brand_sku_tokens

    def possible_brands(string):
        for brand in unique_brand:
            for token in brand_sku_tokens[brand]:
                if token in string:
                    return brand
        return np.nan

    df_name['possible_brand'] = df_name.clean_name.str.replace(' ', '').apply(possible_brands)

    df_name.to_csv('datasets/processed/product_name.tsv', sep='\t', index=False)
    catalog.to_csv('datasets/processed/product_catalog.tsv', sep='\t', index=False)

if __name__ == '__main__':
    main()
