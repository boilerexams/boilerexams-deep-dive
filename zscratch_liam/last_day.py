import matplotlib.pyplot as plt
import polars as pl

import src  # noqa

dfs = src.load_tables()
df_course = dfs["course"]
df_exam = dfs["exam"]
df_question = dfs["question"]
df_submission = dfs["submission"]


df = df_submission.join(df_question, left_on="questionId", right_on="id").join(
    df_course, left_on="courseId", right_on="id"
)
df = df.with_columns(
    (pl.col("abbreviation") + " " + df["number_right"].cast(pl.String)).alias(
        "course_name"
    )
)
d = df.filter(pl.col("questionId") == "f1a35d0f-4e3f-4a17-b163-0d89af9c21d2").limit(1)

for d in d.to_dicts():
    for k, v in d.items():
        print(k, v)


dfcs = df.select(pl.col("timeStarted").dt.date()).unique("timeStarted")

for i, course in enumerate(df["course_name"].unique()):
    dfc = df.filter(pl.col("course_name") == course)
    dfc = (
        dfc.group_by(pl.col("timeStarted").dt.date())
        .agg(pl.count())
        .sort("count")
        .rename({"count": course})
    )
    dfcs = dfcs.join(dfc, on="timeStarted", how="full", suffix=course).drop(
        f"timeStarted{course}"
    )

dfcs = (
    dfcs.fill_null(0)
    .sort("timeStarted")
    .filter(pl.col("timeStarted") > pl.date(2020, 1, 1))
)

# print(dfcs)
# endd

courses = [x for x in dfcs.columns if x != "timeStarted"]


plt.stackplot(
    dfcs["timeStarted"],
    *[dfcs.select(course).to_numpy().flatten() for course in courses],
)
# plt.legend()
plt.show()

# print(
#     df.group_by("course_name").agg(
#         pl.col('timeStarted').filter(pl.col("timeStarted") > pl.date(2024, 10, 1)).count().alias('count')
#     ).sort('count')
# )
