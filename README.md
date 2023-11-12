# DSW - Party Parrot

Pre-made datasets:
1. `datasets/processed/`
2. `datasets/raw/product_name.xlsx` (from the competition e-mail)
3. `datasets/raw/product_catalog.xlsx` (from the competition e-mail)

Run script --> output:
1. `01_cleaning.py` --> `datasets/processed/product_name.tsv`, `datasets/processed/product_catalog.tsv` (clean data)
2. `02.lev_search.py` --> `datasets/processed/result_lev.tsv`
3. `03.fuzzy_search.py` --> `datasets/processed/result_fuzzy.tsv`
