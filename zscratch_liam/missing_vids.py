import src
import polars as pl

pl.Config().set_tbl_rows(100)

print(
    src.exec_postgres_query("""SELECT "table_name" FROM information_schema.tables 
WHERE table_schema = 'public'""")
)
endd

dfc = src.load_tables("course")
dfq = src.load_tables("question")
dfe = src.load_tables("explanation")

print(dfe)
