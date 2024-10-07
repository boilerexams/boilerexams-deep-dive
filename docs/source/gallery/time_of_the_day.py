"""
Time Of Day Studying Density
============================

Comparing MacOS and Windows studying activity throughout the day
"""

from datetime import timedelta as dt

import matplotlib.pyplot as plt
import polars as pl

import src  # noqa

df = src.load_tables("Submission")

df = df.with_columns(pl.col("timeStarted") - dt(hours=6))

for os_family in ["Windows", "OS X"]:
    dfi = df.filter(
        (pl.col("osFamily") == os_family)
        & (
            pl.col("timeStarted").dt.hour() + pl.col("timeStarted").dt.minute() / 60
            != 18.0
        )
    )
    hour_of_day = dfi["timeStarted"].dt.hour() + dfi["timeStarted"].dt.minute() / 60

    plt.hist(hour_of_day, bins=720, alpha=0.5, label=os_family, density=True)

plt.legend()
plt.xlabel("hour of day")
plt.ylabel("submission density")

plt.show()
