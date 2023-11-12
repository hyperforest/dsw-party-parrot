# DSW - Party Parrot

Pre-made datasets/directory:
1. `datasets/processed/`
2. `datasets/raw/product_name.xlsx` (from the competition e-mail)
3. `datasets/raw/product_catalog.xlsx` (from the competition e-mail)

How to run:
0. Run `pip install -r requirements.txt` or `conda install --file environment.yml` to install dependencies
1. `python 01_cleaning.py` --> `datasets/processed/product_name.tsv`, `datasets/processed/product_catalog.tsv` (clean data)
2. `python 02_lev_search.py` --> `datasets/processed/result_lev.tsv`
3. `python 03_fuzzy_search.py` --> `datasets/processed/result_fuzzy.tsv`
4. Run `03_finalize.ipynb`

# Final result
`datasets/final_result.tsv`