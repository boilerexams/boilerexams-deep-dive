import polars as pl
from pathlib import Path
import src

# Step 1: Load the video resources data, selecting only those with 'VIDEO' type
query = """SELECT data, "explanationId" FROM public."Resource" WHERE "type" = 'VIDEO'"""
dfr = src.exec_postgres_query(query)

# Parse and store video link in a separate column
dfr = dfr.with_columns(yt_link=pl.col("data").struct[0]).drop("data")

# Load other relevant tables
dfq = src.load_tables("question")
dfea = src.load_tables("exam")
dfex = src.load_tables("explanation")
dft = src.load_tables("topic")
dfqtt = src.load_tables("questiontotopic")

# Filter and rename columns as needed
dfq = dfq.select([pl.col("id").alias("questionId"), pl.col("examId"), pl.col("courseId")])
dfea = dfea.select([pl.col("id").alias("examId"), pl.col("courseId"), pl.col("season"), pl.col("year")])
dft = dft.select([pl.col("id").alias("topicId"), pl.col("name").alias("topicName"), pl.col("courseId")])
dfqtt = dfqtt.select([pl.col("questionId"), pl.col("topicId")])
dfex = dfex.select([pl.col("id").alias("explanationId"), pl.col("questionId")])

# Step 2: Join data frames to include exams and topics
df_q_e = dfq.join(dfea, on=["examId", "courseId"], how="left")
df_q_e_qtt = df_q_e.join(dfqtt, on="questionId", how="left")
df_q_e_qtt_t = df_q_e_qtt.join(dft, on=["topicId", "courseId"], how="left")

# Step 3: Add explanationId to the main DataFrame
main_df = df_q_e_qtt_t.join(dfex, on="questionId", how="left")

# Step 4: Join with the resources to include video links
final_df = main_df.join(dfr, on="explanationId", how="left")

# Step 5: Filter rows with missing videos (yt_link is null)
df_missing_videos = final_df.filter(pl.col("yt_link").is_null())

# Step 6: Aggregate missing video counts by topic and exam
df_topic_summary = (
    df_missing_videos
    .group_by("topicName")
    .agg([pl.count("questionId").alias("missing_videos_count")])
)

df_exam_summary = (
    df_missing_videos
    .group_by("examId")
    .agg([pl.count("questionId").alias("missing_videos_count")])
)

# Save results to Parquet files for further analysis or reporting
current_dir = Path.cwd()
topic_file_path = current_dir / "missing_videos_by_topic.parquet"
exam_file_path = current_dir / "missing_videos_by_exam.parquet"
df_topic_summary.write_parquet(topic_file_path)
df_exam_summary.write_parquet(exam_file_path)

print(f"Topic summary written to {topic_file_path}")
print(f"Exam summary written to {exam_file_path}")

# Summarize total missing videos across topics and exams
total_missing_videos = df_topic_summary["missing_videos_count"].sum()
print(f"Total missing videos: {total_missing_videos}")
