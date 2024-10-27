import polars as pl

import src

df = src.load_tables("submission")
df = df.with_columns(not_attempted=pl.col("userSolution").list.len() == 0)
print(df["not_attempted"].sum() / df.height * 100)
