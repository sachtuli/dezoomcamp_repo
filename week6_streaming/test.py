import pandas as pd
from tabulate import tabulate
pd.set_option('display.max_rows', 10)
df = pd.read_parquet('C:/Users/d988247/personal/dezoomcamp_repo/week6_streaming/0000_part_00.parquet',engine='pyarrow')
# print(df.head())
print(tabulate(df, headers = 'keys', tablefmt = 'fancy_grid'))