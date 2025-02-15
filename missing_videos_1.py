import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output
import polars as pl
import pandas as pd

# Load Data
df = pl.read_parquet("missing_videos.parquet")

# Ensure 'Course Name' column is available
#if "Course Name" not in df.columns:
#    raise ValueError("The column 'Course Name' is not present in the DataFrame.")

# Get unique course names
course_names = ["All"] + sorted(df["Course Name"].unique().to_list())

# Columns that can be sorted
columns_to_sort = ["Average Percent Correct", "Total Submissions", "Correct Submissions"]

# Initialize Dash app
app = dash.Dash(__name__)

app.layout = html.Div([
    html.Label("Class:"),
    dcc.Dropdown(
        id="class-dropdown",
        options=[{"label": name, "value": name} for name in course_names],
        value="All"
    ),
    
    html.Label("Sort Column:"),
    dcc.Dropdown(
        id="sort-column-dropdown",
        options=[{"label": col, "value": col} for col in columns_to_sort],
        value=columns_to_sort[0]
    ),
    
    html.Label("Sort Order:"),
    dcc.RadioItems(
        id="sort-order-toggle",
        options=[
            {"label": "Ascending", "value": "Ascending"},
            {"label": "Descending", "value": "Descending"}
        ],
        value="Descending",
        inline=True
    ),
    
    dash_table.DataTable(
        id="data-table",
        columns=[{"name": col, "id": col} for col in df.columns],
        page_size=20,
        style_table={"overflowX": "auto"},
    )
])

@app.callback(
    Output("data-table", "data"),
    Input("class-dropdown", "value"),
    Input("sort-column-dropdown", "value"),
    Input("sort-order-toggle", "value")
)
def update_table(selected_class, sort_column, sort_order):
    # Clone the DataFrame to avoid modifying the original
    filtered_df = df.clone()
    
    # Filter by selected class
    if selected_class != "All":
        filtered_df = filtered_df.filter(filtered_df["Course Name"] == selected_class)
    
    # Sort DataFrame
    ascending = sort_order == "Ascending"
    filtered_df = filtered_df.sort(by=[sort_column], descending=[not ascending])
    
    # Convert to Pandas for Dash table compatibility
    return filtered_df.head(20).to_pandas().to_dict("records")

# Run the app
if __name__ == "__main__":
    app.run_server(debug=True)
