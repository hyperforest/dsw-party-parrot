import fuzzywuzzy as fuzz

def compute_fuzzy(product_name, product_sku):
    return fuzz.partial_ratio(product_name, product_sku)
