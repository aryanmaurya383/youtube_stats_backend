import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2
from sqlalchemy import Date

# --------- CONFIGURATION ----------
csv_file = 'all_month_yt_data.csv'
db_name = 'youtube_stats'
db_user = 'postgres'
db_password = 'aryan'
db_host = 'localhost'
db_port = '5432'
table_name = 'yt'
# ----------------------------------

# Step 1: Connect to default database to create new database
conn_default = psycopg2.connect(
    dbname='postgres',
    user=db_user,
    password=db_password,
    host=db_host,
    port=db_port
)
conn_default.autocommit = True
cur = conn_default.cursor()

# Create database if not exists
cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")
exists = cur.fetchone()
if not exists:
    cur.execute(f'CREATE DATABASE {db_name}')
    print(f"Database '{db_name}' created.")
else:
    print(f"Database '{db_name}' already exists.")
cur.close()
conn_default.close()

# Step 2: Read CSV and convert timestamp
df = pd.read_csv(csv_file, dtype={'title': str}, low_memory=False)
# Convert timestamp to datetime and extract date
df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce').dt.date

# Step 3: Connect to new database and load data
engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
# Specify DATE type for timestamp column
df.to_sql(
    table_name,
    engine,
    if_exists='replace',
    index=False,
    dtype={'timestamp': Date()}
)
print(f"Data loaded into table '{table_name}'.")

# Step 4: Create indexes
with psycopg2.connect(
    dbname=db_name,
    user=db_user,
    password=db_password,
    host=db_host,
    port=db_port
) as conn:
    cur = conn.cursor()

    # Individual column indexes
    for col in ['timestamp', 'country', 'category']:
        index_name = f"idx_{table_name}_{col}"
        cur.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({col});")
        print(f"Created index: {index_name}")

    # GIN index for tags array
    gin_index_name = f"idx_{table_name}_tags_gin"
    cur.execute(f"""
        CREATE INDEX IF NOT EXISTS {gin_index_name}
        ON {table_name} USING GIN ((string_to_array(tags, '|')));
    """)
    print(f"Created GIN index: {gin_index_name}")

    # Composite index
    composite_index_name = f"idx_{table_name}_country_cat_ts"
    cur.execute(f"""
        CREATE INDEX IF NOT EXISTS {composite_index_name}
        ON {table_name} (country, category, timestamp);
    """)

    composite_index_name = f"idx_{table_name}_country_ts"
    cur.execute(f"""
        CREATE INDEX IF NOT EXISTS {composite_index_name}
        ON {table_name} (country, timestamp);
    """)

    composite_index_name = f"idx_{table_name}_cat_ts"
    cur.execute(f"""
        CREATE INDEX IF NOT EXISTS {composite_index_name}
        ON {table_name} (category, timestamp);
    """)
    print(f"Created composite index: {composite_index_name}")

    conn.commit()
    cur.close()

print("All operations completed successfully.")