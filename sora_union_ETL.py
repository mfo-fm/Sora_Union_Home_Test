import psycopg2
import pandas as pd

# ===========================
# Step 1: Database Connection(Using default database postgres)
# ===========================
conn = psycopg2.connect(
    host = "localhost",
    port = "5432",
    database = "postgres",  
    user = "postgres",
    password = "Farouq9595"
)
cur = conn.cursor()

# Creating the schema if it doesn't exist
cur.execute("CREATE SCHEMA IF NOT EXISTS sora_union_data_warehouse;")
cur.execute("SET search_path TO sora_union_data_warehouse;")
conn.commit()
print("Schema 'sora_union_data_warehouse' created and set as default.")

# ===========================
# Step 2: Table Creation
# ===========================
schema_queries = """
CREATE TABLE IF NOT EXISTS dim_project (
    project_id SERIAL PRIMARY KEY,
    project_name TEXT,
    client_name TEXT
);

CREATE TABLE IF NOT EXISTS dim_team_member (
    team_member_id SERIAL PRIMARY KEY,
    team_member_name TEXT,
    role TEXT
);

CREATE TABLE IF NOT EXISTS fact_task_tracking (
    fact_id SERIAL PRIMARY KEY,
    project_id INT REFERENCES dim_project(project_id),
    client_name TEXT,
    team_member_id INT REFERENCES dim_team_member(team_member_id),
    task TEXT,
    start_date DATE,
    end_date DATE,
    estimated_hours FLOAT,
    date_worked DATE,
    logged_hours FLOAT,
    note TEXT,
    billable_hours FLOAT
);

CREATE TABLE IF NOT EXISTS float_table (
    client_name TEXT,
    project_name TEXT,
    role TEXT,
    name TEXT,
    task TEXT,
    start_date DATE,
    end_date DATE,
    estimated_hours FLOAT
);

CREATE TABLE IF NOT EXISTS clickup_table (
    client_name TEXT,
    project_name TEXT,
    name TEXT,
    task TEXT,
    date DATE,
    hours FLOAT,
    note TEXT,
    billable TEXT
);
"""
cur.execute(schema_queries)
conn.commit()
print("Schema and tables created successfully.")

# Here I create a function to truncate tables incase I want to rerun and get the integrity and avoid duplicate key errors
def truncate_tables():
    truncate_queries = [
        "TRUNCATE TABLE dim_project RESTART IDENTITY CASCADE;",
        "TRUNCATE TABLE dim_team_member RESTART IDENTITY CASCADE;",
        "TRUNCATE TABLE fact_task_tracking RESTART IDENTITY CASCADE;",
        "TRUNCATE TABLE float_table RESTART IDENTITY CASCADE;",
        "TRUNCATE TABLE clickup_table RESTART IDENTITY CASCADE;"
    ]
    for query in truncate_queries:
        cur.execute(query)
    conn.commit()
    print("All tables truncated successfully.")

truncate_tables()

# ===========================
# Step 3: Extract Data
# ===========================
float_data = pd.read_csv(r'C:\Users\Farouq.Olaniyan\Documents\Farouq_personal\Sora_Union\Float_allocations.csv')
clickup_data = pd.read_csv(r'C:\Users\Farouq.Olaniyan\Documents\Farouq_personal\Sora_Union\ClickUp_clickup.csv')

print(f"Records in Float Data file: {len(float_data)}")
print(f"Records in ClickUp Data file: {len(clickup_data)}")

# Rename columns to match schema
float_data.rename(columns = {
    'Client': 'client_name',
    'Project': 'project_name',
    'Role': 'role',
    'Name': 'name',
    'Task': 'task',
    'Start Date': 'start_date',
    'End Date': 'end_date',
    'Estimated Hours': 'estimated_hours'
}, inplace=True)

clickup_data.rename(columns = {
    'Client': 'client_name',
    'Project': 'project_name',
    'Name': 'name',
    'Task': 'task',
    'Date': 'date',
    'Hours': 'hours',
    'Note': 'note',
    'Billable': 'billable'
}, inplace = True)

# ===========================
# Step 4: Load Raw Data
# ===========================
def load_raw_data(dataframe, table_name):
    cur.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;")
    for _, row in dataframe.iterrows():
        placeholders = ', '.join(['%s'] * len(row))
        columns = ', '.join(row.index)
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        cur.execute(sql, tuple(row))
    conn.commit()
    print(f"Records loaded into {table_name}: {len(dataframe)}")

load_raw_data(float_data, 'float_table')
load_raw_data(clickup_data, 'clickup_table')

# ===========================
# Step 5: Transform Data
# ===========================
# Dim_Project
dim_project = float_data[['client_name', 'project_name']].drop_duplicates().reset_index(drop = True)
dim_project['project_id'] = dim_project.index + 1
print(f"Records in Dim_Project: {len(dim_project)}")

# Dim_TeamMember
dim_team_member = float_data[['name', 'role']].drop_duplicates().reset_index(drop = True)
dim_team_member.rename(columns={'name': 'team_member_name'}, inplace = True)
dim_team_member['team_member_id'] = dim_team_member.index + 1
print(f"Records in Dim_TeamMember: {len(dim_team_member)}")

# Merge Float Data and ClickUp Data
fact_data = clickup_data.merge(float_data, on=['name', 'project_name', 'task'], how = 'left')

# Fix client_name if columns are duplicated during merge
if 'client_name_x' in fact_data.columns:
    fact_data['client_name'] = fact_data['client_name_x']
elif 'client_name_y' in fact_data.columns:
    fact_data['client_name'] = fact_data['client_name_y']

# Merge with Dimensional Tables
fact_data = fact_data.merge(dim_project, on = ['project_name', 'client_name'], how = 'left')  
fact_data = fact_data.merge(dim_team_member, left_on = 'name', right_on='team_member_name', how = 'left')  

# Populating The Fact Table
fact_task_tracking = fact_data[[ 
    'project_id', 'client_name', 'team_member_id', 'task', 'start_date',
    'end_date', 'estimated_hours', 'date', 'hours', 'note', 'billable'
]].rename(columns={ 
    'date': 'date_worked',
    'hours': 'logged_hours'
})

fact_task_tracking['billable_hours'] = fact_task_tracking.apply(
    lambda row: row['logged_hours'] if row['billable'] == "Yes" else 0, axis = 1
)
fact_task_tracking.drop(columns = ['billable'], inplace = True)

# Replacing NaT values with None for datetime fields
fact_task_tracking['start_date'] = pd.to_datetime(fact_task_tracking['start_date'], errors = 'coerce').dt.date
fact_task_tracking['end_date'] = pd.to_datetime(fact_task_tracking['end_date'], errors = 'coerce').dt.date

# Convert NaT to None for database insertion
fact_task_tracking = fact_task_tracking.where(pd.notnull(fact_task_tracking), None)

# Here I want to ensure numeric fields are properly filled and converted
fact_task_tracking['estimated_hours'] = fact_task_tracking['estimated_hours'].fillna(0).astype(float)
fact_task_tracking['logged_hours'] = fact_task_tracking['logged_hours'].fillna(0).astype(float)
fact_task_tracking['billable_hours'] = fact_task_tracking['billable_hours'].fillna(0).astype(float)
fact_task_tracking['note'] = fact_task_tracking['note'].fillna('')

# ===========================
# Step 6: Load Dimensional and Fact Tables
# ===========================
def load_table(dataframe, table_name):
    cur.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;")
    for _, row in dataframe.iterrows():
        placeholders = ', '.join(['%s'] * len(row))
        columns = ', '.join(row.index)
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        cur.execute(sql, tuple(row))
    conn.commit()
    print(f"Records loaded into {table_name}: {len(dataframe)}")

# Load Tables
load_table(dim_project, 'dim_project')
load_table(dim_team_member, 'dim_team_member')
load_table(fact_task_tracking, 'fact_task_tracking')

print("ETL process completed successfully!")

cur.close()
conn.close()
