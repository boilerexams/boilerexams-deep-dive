import src
from pathlib import Path

# Define the SQL query to count distinct question IDs with explanations and resources
query = """
SELECT COUNT(DISTINCT q.id) AS distinct_question_count
FROM "Question" q 
JOIN "Explanation" e ON e."questionId" = q.id 
JOIN "Resource" r ON r."explanationId" = e.id;
"""

# Execute the query
result_df = src.exec_postgres_query(query)

# Define the path for the output file
output_path = Path.cwd() / "distinct_question_count.parquet"

# Write the result to a Parquet file
result_df.write_parquet(output_path)

print(f"Distinct question count saved to {output_path}")
