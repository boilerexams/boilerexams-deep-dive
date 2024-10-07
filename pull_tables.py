import src

table_names = [
    "Question",
    "Exam",
    "Topic",
    "QuestionToTopic",
    "Course",
    "Feedback",
    "Application",
    "CourseRequest",
    "AnswerChoice",
    "Session",
    "Explanation",
    "Event",
    # "Submission",
    "Explanation",
    "AnswerChoice",
]

src.save_tables(table_names)
