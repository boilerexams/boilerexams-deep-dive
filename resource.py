import src
import polars as pl

# query = """
# SELECT *
#   FROM information_schema.columns
#  WHERE table_name   = 'Resource'
#      ;
# """

# print(src.exec_postgres_query(query))
# endd

query = """SELECT data, "explanationId"
FROM public."Resource" 
WHERE "type" = 'VIDEO'
"""

dfr = src.exec_postgres_query(query)

dfr = dfr.with_columns(yt_link=pl.col("data").struct[0]).drop("data")
dfq = src.load_tables("question")
dfe = src.load_tables("explanation")
dfc = src.load_tables("course")

dfq = (
    dfq.join(dfe, left_on="id", right_on="questionId")
    .join(dfr, left_on="id_right", right_on="explanationId", how="left")
    .join(dfc, left_on="courseId", right_on="id")
    .with_columns(
        course_name=pl.col("abbreviation") + " " + pl.col("number_right").cast(str)
    )
)

dfq = (
    dfq.group_by("course_name")
    .agg(
        pl.col("yt_link").count().alias("question_videos"),
        pl.col("yt_link").is_null().sum().alias("missing_question_videos"),
        pl.col("id").count().alias("total_questions"),
    )
    .sort("missing_question_videos")
)

dfq = dfq.with_columns(
    (100 * pl.col("missing_question_videos") / pl.col("total_questions")).alias(
        "missing_percent"
    )
)

total_missing_question_videos = dfq["missing_question_videos"].sum()
total_videos = dfq["question_videos"].sum()
total_questions = dfq["total_questions"].sum()
print(total_missing_question_videos)
print(total_videos)
print(total_missing_question_videos / total_questions * 100)

pl.Config().set_tbl_rows(100)
# print(dfq)
