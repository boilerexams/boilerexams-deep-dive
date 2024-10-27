import src
import polars as pl
import matplotlib.pyplot as plt
import datetime
import numpy as np

video_lengths = """0:9
2:31
0:56
1:21
1:15
0:46
1:55
0:9
0:17
0:14
0:54
0:58
0:43
1:55
1:34
0:13
0:55
1:29
1:30
1:03
1:14
1:38"""
print(sum([float(x[0]) * 60 + float(x[2:]) for x in video_lengths.splitlines()]) / 60)
endd

df_apps = src.load_tables("Application")
df_apps = df_apps.filter(pl.col("name") != "Kaden")

print(df_apps)

df_feed = src.load_tables("Feedback")

print(df_feed.group_by("type").len())
endd

df_feed = df_feed.filter(pl.col("type") == "QUESTION_REPORT").sort("createdAt")

df_feed = df_feed.filter(
    pl.col("createdAt") > datetime.datetime(2024, 8, 19), pl.col("status") != "OPEN"
).with_columns(timeTaken=pl.col("updatedAt") - pl.col("createdAt"))

hours_taken_to_close = (
    df_feed["timeTaken"].to_numpy().astype(np.float64) / 1e6 / 86400 * 24
)

print(np.percentile(hours_taken_to_close, [5, 50, 95]))

plt.hist(hours_taken_to_close / 24, bins=50)
plt.show()
