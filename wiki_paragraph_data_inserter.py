import json
import glob
import snowflake.connector
from account_snowflake_reach import *  # your credentials

# connect to Snowflake
ctx = snowflake.connector.connect(
    user=user,
    password=password,
    account=account,
    warehouse=WAREHOUSE,
    database=DATABASE,
    schema=SCHEMA,
    role=ROLE
)
cs = ctx.cursor()

# table name
TABLE_NAME = "kb_paragraphs"

# make sure table exists
cs.execute(f"""
CREATE OR REPLACE TABLE {TABLE_NAME} (
    PARAGRAPH_ID STRING,
    PAGE_TITLE STRING,
    STT_TEXT STRING,
    EMBEDDING ARRAY
)
""")

# path to your extracted Wikipedia JSON files
json_files_path = "extracted/*/wiki_*.json"

# loop through JSON files and insert paragraphs
for filepath in glob.glob(json_files_path):
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            page = json.loads(line)
            # escape single quotes for SQL
            safe_title = page['title'].replace("'", "''")
            safe_text = page['text'].replace("'", "''")
            paragraph_id = str(page['id'])

            try:
                sql = f"""
                INSERT INTO {TABLE_NAME} (PARAGRAPH_ID, PAGE_TITLE, STT_TEXT, EMBEDDING)
                VALUES ('{paragraph_id}', '{safe_title}', '{safe_text}', AI_EMBED('{safe_text}'))
                """
                cs.execute(sql)
            except Exception as e:
                print(f"Error inserting paragraph {paragraph_id}: {e}")

cs.close()
ctx.close()

print("Wikipedia paragraphs inserted successfully!")
