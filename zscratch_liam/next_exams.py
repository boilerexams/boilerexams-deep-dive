import src
import datetime
import polars as pl

dfc = src.load_tables("course").select("id", "number", "abbreviation")
dfe = (
    src.load_tables("event")
    .select("start", "end", "courseId")
    .join(dfc, left_on="courseId", right_on="id")
    .drop("courseId")
    .with_columns(pl.col("start").dt.replace_time_zone("UTC"))
    .with_columns(
        course_name=pl.col("abbreviation") + " " + pl.col("number").cast(str),
        day_of_week=pl.col("start").dt.to_string("%A"),
    )
)

today = datetime.datetime.now(tz=datetime.timezone.utc)
horizon = datetime.timedelta(days=7)

dfe = dfe.select("course_name", "day_of_week", "start")

print(
    dfe.filter(pl.col("start") > today, pl.col("start") < today + horizon)
    .unique("course_name")
    .sort("start")
    .with_columns(start=pl.col("start").dt.convert_time_zone("EST"))
)
