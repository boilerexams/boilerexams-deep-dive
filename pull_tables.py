import src

table_names = [
    "Question",
    "Exam",
    "Topic",
    "QuestionToTopic",
    "Course",
    "Feedback",
    "CourseRequest",
    "AnswerChoice",
    "Session",
    "Explanation",
    "Event",
    "Explanation",
    "AnswerChoice",
    # "Submission",
]

src.save_tables(table_names)
