"""
Plots
=====
"""

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import src  # noqa

df = src.load_tables("Submission").to_pandas()

reasonable_watch_m = 10
reasonable_watch_s = reasonable_watch_m * 60
reasonable_watch_ms = reasonable_watch_s * 1000
ms_to_hr = 3600000

ok_study_inds = (df["timeSpent"] < reasonable_watch_ms) & (df["timeSpent"] > 0)
not_attempted_videos = df["timeSpentModal"] <= 0
video_too_long = df["timeSpentModal"] > reasonable_watch_ms
ok_study_inds_video = ~video_too_long & (df["timeSpentModal"] > 0)
thrown_out_videos = video_too_long | not_attempted_videos

print(f"Total number of submissions: {len(df)}")
print(f'Total video submissions: {np.sum(df["timeSpentModal"] > 0)}')
print(f'Total time taken: {df["timeSpent"].sum() / ms_to_hr:.2f} hours')

total_study_time = df["timeSpent"][ok_study_inds].sum() / ms_to_hr
total_video_time = df["timeSpentVideo"][ok_study_inds_video].sum() / ms_to_hr
print(f"Total study hours < {reasonable_watch_m} mins: {total_study_time:.2f}")
print(f"Total video hours < {reasonable_watch_m} mins: {total_video_time:.2f}")

mean_study_time = df["timeSpent"][ok_study_inds].mean() / ms_to_hr * 60
print(f"Mean of valid study times: {mean_study_time:.2f} minutes")
mean_video_time = df["timeSpentVideo"][ok_study_inds_video].mean() / ms_to_hr * 60
print(f"Mean of valid video times: {mean_video_time:.2f} minutes")

extra_discounted_study = np.sum(~ok_study_inds) * mean_study_time / 60
print(
    f"Extra time due to discounted study indices: {extra_discounted_study :.2f} hours"
)
extra_discounted_video = np.sum(video_too_long) * mean_video_time / 60
print(
    f"Extra time due to discounted video indices: {extra_discounted_video :.2f} hours"
)
total_time = (
    total_study_time
    + total_video_time
    + extra_discounted_study
    + extra_discounted_video
)
print(f"Total time: {total_time:.2f} hours")

# time of the day
df["timeStarted"] = pd.to_datetime(df["timeStarted"])
# hour of day
df["hour"] = (
    df["timeStarted"].dt.hour + df["timeStarted"].dt.minute / 60 - 5
)  # UTC to EST
df["hour"] = df["hour"] % 24
# day of week
df["day"] = df["timeStarted"].dt.dayofweek

# %%
# Submissions in the past 24 hours
last_24_hr_inds = df["timeStarted"] > pd.Timestamp.now() - pd.Timedelta(days=1)
print(f"Submissions in the past 24 hours: {last_24_hr_inds.sum()}")
plt.figure()
plt.hist(df["hour"][last_24_hr_inds], bins=24)
plt.title("Submissions in the past 24 hours")
plt.xlabel("Hour of day")
plt.ylabel("Submission count")
plt.xticks(
    [0, 3, 6, 9, 12, 15, 18, 21],
    ["12am", "3am", "6am", "9am", "12pm", "3pm", "6pm", "9pm"],
)
plt.show()

# %%
# 2D heatmap of time of day and day of week

plt.figure()
plt.hist2d(df["hour"][ok_study_inds], df["day"][ok_study_inds], bins=(50, 7))
plt.gca().invert_yaxis()
plt.colorbar(label="Submission count")
plt.yticks([0, 1, 2, 3, 4, 5, 6], ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"])
plt.xticks(
    [0, 3, 6, 9, 12, 15, 18, 21],
    ["12am", "3am", "6am", "9am", "12pm", "3pm", "6pm", "9pm"],
)
plt.show()

# %%
# Plotting the mean time spent over the day

hr_space = np.linspace(0, 23, 1000)
vals = np.zeros(len(hr_space))
for i, hour in enumerate(hr_space):
    inds = (df["hour"] > hour - 0.5) & (df["hour"] < hour + 0.5) & ok_study_inds
    vals[i] = df["timeSpent"][inds].mean() / ms_to_hr * 60

plt.figure()
plt.plot(hr_space, vals)
plt.title("Mean time spent")
plt.xticks(
    [0, 3, 6, 9, 12, 15, 18, 21],
    ["12am", "3am", "6am", "9am", "12pm", "3pm", "6pm", "9pm"],
)
plt.ylabel("(min)")
plt.show()

# %%
# Plotting correctness over the day

vals = np.zeros(len(hr_space))
for i, hour in enumerate(hr_space):
    inds = (df["hour"] > hour - 0.5) & (df["hour"] < hour + 0.5)
    vals[i] = df["correct"][inds].mean()

plt.figure()
plt.plot(hr_space, vals)
plt.title("Mean accuracy over the day")
plt.xticks(
    [0, 3, 6, 9, 12, 15, 18, 21],
    ["12am", "3am", "6am", "9am", "12pm", "3pm", "6pm", "9pm"],
)
plt.ylabel("Accuracy")
plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:.0%}"))
plt.show()

# %%
# Plot the time taken to complete each question and watch the video

plt.figure(figsize=(10, 4))
plt.subplot(1, 2, 1)
plt.hist(df["timeSpent"][ok_study_inds] / ms_to_hr, bins=100)
plt.title("Question Time")
plt.xlabel("(hr)")
plt.ylabel("count")
plt.yscale("log")
plt.subplot(1, 2, 2)
plt.hist(df["timeSpentModal"][ok_study_inds_video] / ms_to_hr, bins=100)
plt.title("Video Watch Time")
plt.xlabel("(hr)")
plt.ylabel("count")
plt.yscale("log")
plt.tight_layout()
plt.show()
