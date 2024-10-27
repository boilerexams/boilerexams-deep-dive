import polars as pl
from pathlib import Path
import src

query = """SELECT data, "explanationId"
FROM public."Resource"
WHERE "type" = 'VIDEO'
"""

dfr = src.exec_postgres_query(query)
current_dir = Path.cwd()

# 2. Create the full path for the output file
file_path = current_dir / "resources.parquet"
dfr.write_parquet(file_path)
url_values = dfr.select(pl.col("data").struct["url"] == "")
print(url_values.unique())
dfr = dfr.with_columns(yt_link=pl.col("data").struct[0]).drop("data")
#print(dfr["yt_link"])
print(dfr.null_count())

dfq = src.load_tables("question")
dfea = src.load_tables("exam")
dfex = src.load_tables("explanation")
dfc = src.load_tables("course")
dft = src.load_tables("topic")
dfqtt = src.load_tables("questiontotopic")

dfq = dfq.select([
    pl.col("id").alias("questionId"),
    pl.col("examId"),
    pl.col("courseId")
])

dfea = dfea.select([
    pl.col("id").alias("examId"), 
    pl.col("courseId"), 
    pl.col("season"), 
    pl.col("year")
])
dft = dft.select([
    pl.col("id").alias("topicId"), 
    pl.col("name").alias("topicName"), 
    pl.col("courseId")
])
dfqtt = dfqtt.select([
    pl.col("questionId"), 
    pl.col("topicId")
])
dfex = dfex.select([
    pl.col("id").alias("explanationId"),
    pl.col("questionId")
])

# Step 2: Join dfq and dfe on 'questionId' and 'examId'
df_q_e = dfq.join(dfea, on=["examId", "courseId"], how="inner")

# Step 3: Join the result with dfqtt on 'questionId'
df_q_e_qtt = df_q_e.join(dfqtt, on="questionId", how="inner")

# Step 4: Join the result with dft on 'topicId' and 'courseId'
df_q_e_qtt_t = df_q_e_qtt.join(dft, on=["topicId", "courseId"], how="inner")

# Step 5: Join with dfex on 'questionId' to add explanationId
main_df = df_q_e_qtt_t.join(dfex, on="questionId", how="inner")

#Step 6: Join the main dataframe with the resources on the explanationId
final_df = main_df.join(dfr, on="explanationId", how = "outer")

# Print the final DataFrame
print(final_df)

#make into feature

#Filter for rows where 'yt_link' is null
df_missing_videos = final_df.filter(
    pl.col("yt_link").is_null()
)


# Group by 'topicId' and 'examId' to count missing videos
df_summary = (
    df_missing_videos
    .group_by("topicName")
    .agg([
        pl.count("questionId").alias("missing_videos_count")
    ])
)

""" # Join with the topic table to get topic names
df_result = df_summary.join(
    dft.select(["topicId", "name"]), 
    on="topicId", 
    how="left"
) """

#Show the final result
print(df_summary)


current_dir = Path.cwd()

# 2. Create the full path for the output file
file_path = current_dir / "output_file.parquet"

df_summary.write_parquet(file_path)
"""
dfq = (
    dfq.join(dfex, left_on="id", right_on="questionId")
    .join(dfr, left_on="id_right", right_on="explanationId", how="left")
    .join(dfc, left_on="courseId", right_on="id")
    .join(dft, left_on="courseId", right_on="id", how="left")
    .with_columns(
        course_name=pl.col("abbreviation") + " " + pl.col("number_right").cast(str)
    )
)





#print(dfq)
#print(dfq.columns)
dfq = (
    dfq.group_by("course_name")
    .agg(
        #
        pl.col("yt_link").count().alias("question_videos"),
        pl.col("yt_link").is_null().sum().alias("missing_question_videos"),
        pl.col("id").count().alias("total_questions"),
    )
    .sort("missing_question_videos")
)
print(dfq)
dfq = dfq.with_columns(
    (100 * pl.col("missing_question_videos") / pl.col("total_questions")).alias(
        "missing_percent"
    )
)

total_missing_question_videos = dfq["missing_question_videos"].sum()
total_videos = dfq["question_videos"].sum()
total_questions = dfq["total_questions"].sum()
##print(total_missing_question_videos)
#print(total_videos)
#print(total_missing_question_videos / total_questions * 100)
#print(dfq)
#print(df_topic_exam)
pl.Config().set_tbl_rows(100)
# print(dfq)

dfq = (
    dfq.group_by("course_name")
    .agg(
        #
        pl.col("yt_link").count().alias("question_videos"),
        pl.col("yt_link").is_null().sum().alias("missing_question_videos"),
        pl.col("id").count().alias("total_questions"),
    )
    .sort("missing_question_videos")
)
"""