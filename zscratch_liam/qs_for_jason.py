import polars as pl

qids = [
    "4769a993-27f2-434e-9996-f3226866b3b7",
    "9ecd2862-fc6a-4844-8db9-16f0a22aca82",
    "ba4b69a2-0795-49ea-b6fb-19d129b43eca",
    "450b56e6-a063-491c-afc8-13c1238c4f21",
    "1ffc6dd1-9d82-4bcf-b2bf-956cfff48340",
    "f6eb6ef7-8146-4f38-a4a8-45abcef92187",
    "795ffb66-7153-420f-bb9e-f82e83608d60",
    "117640eb-671c-48ab-bdea-f70370971267",
    "f1a35d0f-4e3f-4a17-b163-0d89af9c21d2",
    "88538c5d-f96f-4ca9-9b08-9f8b4792ed28",
]

dfq = pl.read_csv("Question.csv")
dfe = pl.read_csv("Exam.csv")
dfc = pl.read_csv("Course.csv")
dft = pl.read_csv("Topic.csv")
dfq2t = pl.read_csv("QuestionToTopic.csv")


df = (
    dfq.filter(pl.col("id").is_in(qids))
    .join(dfc, left_on="courseId", right_on="id")
    .filter(pl.col("subject") == "Chemistry")
    .drop(
        "type",
        "number",
        "disclaimer_right",
        "studyModes",
        "flags_right",
        "color",
        "stats_right",
        "subject",
        "name",
        "abbreviation",
        "disclaimer",
        "flags",
        "stats",
        "bodyVector",
        "number_right",
        "parentId",
    )
    .join(dfq2t.select("questionId", "topicId"), left_on="id", right_on="questionId")
    .join(dft.select("id", "name"), left_on="topicId", right_on="id")
    .unique("id")
)

df = df.drop("examId", "courseId", "topicId", "data")

template = (
    lambda topic,
    id: f"https://www.boilerexams.com/courses/CHM11500/topics/{topic}/{id}"
)

for row in df.iter_rows(named=True):
    print(template(row["name"], row["id"]).replace(" ", "%20"))
