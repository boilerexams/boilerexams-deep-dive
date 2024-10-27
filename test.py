import matplotlib.pyplot as plt
import polars as pl

#Reads different tables
pd_df = pl.read_parquet('tables/Course.parquet')
df = pl.read_parquet('tables/Course.parquet')
question_t = pl.read_parquet("tables/Question.parquet")
exam_t = pl.read_parquet("tables/Exam.parquet")

#submission = pl.read_parquet("tables/Submission.parquet")

#Can add custom course
cid = "15900"
cname ="CS"

filtered_df = df.filter(
     (pl.col('number') == int(cid)) &
     (pl.col('abbreviation') == cname)
  )
if not filtered_df.is_empty():
    id = filtered_df['id'].unique()[0]
else:
    exit("Number or name entered incorrectly")

filtered = question_t.filter(pl.col("courseId") == id)
#print(filtered.columns)
qids = set(filtered['id'])
#print(qids)
""" import matplotlib.pyplot as plt
import polars as pl
import datetime

#Reads different tables
df = pl.read_parquet("tables\\Course.parquet")
question_t = pl.read_parquet("tables\\Question.parquet")
submission = pl.read_parquet("tables\\Submission.parquet")

#Can add custom course and exam date
cid = "15900"
cname ="CS"
exam_d = "2024-04-01 20:00:00"


filtered_df = df.filter(
      (pl.col('number') == int(cid)) &
      (pl.col('abbreviation') == cname)
  )

if not filtered_df.is_empty():
    id = filtered_df['id'].unique()[0]
else:
    exit("Number or name entered incorrectly")

filtered = question_t.filter(pl.col("courseId") == id)
qids = set(filtered['id'])
filtered_df = submission.filter(pl.col("questionId").is_in(qids))
filtered_df = filtered_df.filter(pl.col("userSolution") != [])
delta_0 = datetime.timedelta(hours=0)
delta_48 = datetime.timedelta(hours=48)


filtered_df = filtered_df.with_columns(
    rounded_time=filtered_df["timeEnded"].dt.round("1h")
)

# Filter based on timeEnded being within 48 hours
filtered_df = filtered_df.filter(
    (exam_obj - pl.col("rounded_time")>= delta_0) &
    (exam_obj - pl.col("rounded_time")<= delta_48)
)
print(filtered_df)
correct_num = filtered_df.filter(pl.col("correct") == True)
grouped_df_total = filtered_df.group_by("rounded_time").agg(pl.len())
grouped_df_correct = correct_num.group_by("rounded_time").agg(pl.len())
date_string = submission['timeEnded'][0]
date_string = date_string.replace(microsecond=0)

all_values = grouped_df_total['rounded_time'].to_list()
dict1 = {}
for each in all_values:
    inter = grouped_df_correct.filter(pl.col('rounded_time') == each)
    a = inter['len']
    if(len(a) != 0):
        inter1 = grouped_df_total.filter(pl.col('rounded_time') == each)
        b= inter1['len'][0]
        dict1[each] = a[0]/b
    else:
        dict1[each] = 0

dict2 = {}
for key,val in dict1.items():
    delta = exam_obj-key
    delta = delta.total_seconds()/3600
    dict2[delta] = val

sorted_dict = dict(sorted(dict2.items()))
x = list(sorted_dict.keys())
y = list(sorted_dict.values())

# Create the plot
plt.scatter(x, y, marker='o')
plt.xlabel('Hours till Exam')
plt.ylabel('Student Accuracy')
plt.title('Hours vs Accuracy')
plt.gca().invert_xaxis()
# Show the plot
plt.show() 

 """