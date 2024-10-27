import src
import polars as pl
import matplotlib.pyplot as plt

pl.Config().set_tbl_rows(100)

dfc = src.load_tables("course")

df = src.load_tables("feedback")
df = df.filter(pl.col("type") == "VIDEO_FEEDBACK")
df = df.with_columns(
    reason=pl.col("data").struct[1],
    recommend=pl.col("data").struct[5],
    rating=pl.col("data").struct[6],
)


dfmr = df.group_by("courseId").agg(
    pl.col("rating").mean().alias("mean_rating"),
    pl.col("rating").std().alias("stdev_rating"),
    pl.col("questionId").unique().count().alias("unique_questions"),
    pl.col("rating").filter(pl.col("rating") == 1).count().alias("one_stars"),
    pl.count(),
)

print(
    dfmr.join(
        dfc.select("id", "abbreviation", "number"), left_on="courseId", right_on="id"
    )
    .sort("mean_rating")
    .drop("courseId")
)
endd

print(df.filter(pl.col("rating") == 1).select("reason", "questionId", "createdAt"))
endd

plt.hist(df["rating"], bins=[1, 2, 3, 4, 5, 6], alpha=0.5, density=True)
plt.grid()
plt.xlabel("Video rating")
plt.ylabel("Density")
plt.show()
