"""
Operating System Accuracy
=========================

Which operating system gets the most questions right, on average?
"""

import polars as pl

import src  # noqa

df = src.load_tables("Submission")

df = (
    df.group_by("osFamily")
    .agg(pl.col("correct").cast(pl.Float64).mean() * 100)
    .sort("correct")
    .filter(pl.col("osFamily").str.len_chars() < 10)
    .filter(pl.col("correct") > 0)
    .rename({"correct": "percentCorrect"})
)

print(df)
