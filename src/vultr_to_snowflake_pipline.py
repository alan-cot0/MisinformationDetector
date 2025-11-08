#!/usr/bin/env python3
"""
Streaming ingestion for enwiki-latest-pages-articles.xml -> Snowflake

Notes:
- Designed for very large XML dumps (streaming with iterparse).
- Adjust BATCH_SIZE for performance / memory tradeoffs.
- Minimal cleaning is applied. For better downstream retrieval you'll
  likely want to add wiki-markup cleaning (mwparserfromhell or wikiextractor)
  as a later step.
"""

import re
import os
import sys
import time
import snowflake.connector
import xml.etree.ElementTree as ET
from account_snowflake_reach import user, password, account, DATABASE, SCHEMA

# --------------------
# CONFIG - set these
# --------------------
XML_PATH = "/mnt/blockstorage/enwiki-latest-pages-articles.xml"  # change as needed
SNOWFLAKE_USER = user
SNOWFLAKE_PASSWORD = password
SNOWFLAKE_ACCOUNT = account     # e.g. abc12345.us-east-1
SNOWFLAKE_DATABASE = DATABASE
SNOWFLAKE_SCHEMA = SCHEMA
SNOWFLAKE_TABLE = "kb_paragraphs" #????????????????!!!
BATCH_SIZE = 500                 # rows per insert batch (tune for perf)
MAX_PARAGRAPHS_PER_PAGE = 1000   # safety cap to avoid runaway pages

# --------------------
# Helper utilities
# --------------------
WIKI_NS = "{http://www.mediawiki.org/xml/export-0.11/}"

def split_into_paragraphs(wikitext):
    """Split wiki text into paragraphs. Returns list of cleaned paragraph strings."""
    if not wikitext:
        return []

    # Normalize newlines, convert \r\n -> \n
    text = wikitext.replace("\r\n", "\n").replace("\r", "\n")

    # Collapse repeated blank lines to exactly two newlines then split
    text = re.sub(r"\n\s*\n+", "\n\n", text)

    # split on 2 or more newlines
    parts = [p.strip() for p in re.split(r"\n{2,}", text) if p.strip()]

    # safety: cap number of paragraphs per page
    if len(parts) > MAX_PARAGRAPHS_PER_PAGE:
        parts = parts[:MAX_PARAGRAPHS_PER_PAGE]

    return parts

# --------------------
# Snowflake helpers
# --------------------
def get_sf_connection():
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )

def create_table_if_missing(conn):
    cur = conn.cursor()
    try:
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {SNOWFLAKE_TABLE} (
            id BIGINT AUTOINCREMENT,
            page_id STRING,
            title STRING,
            namespace STRING,
            paragraph_index INTEGER,
            paragraph_text STRING,
            source_filename STRING,
            ingested_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """)
    finally:
        cur.close()

def batch_insert(conn, rows):
    """
    rows: list of tuples (page_id, title, namespace, paragraph_index, paragraph_text, source_filename)
    """
    if not rows:
        return
    cur = conn.cursor()
    try:
        sql = f"INSERT INTO {SNOWFLAKE_TABLE} (page_id, title, namespace, paragraph_index, paragraph_text, source_filename) VALUES (%s, %s, %s, %s, %s, %s)"
        cur.executemany(sql, rows)
        conn.commit()
    finally:
        cur.close()

# --------------------
# XML streaming ingest
# --------------------
def stream_wiki_to_snowflake(xml_path):
    start_time = time.time()
    conn = get_sf_connection()
    create_table_if_missing(conn)

    rows_buffer = []
    total_paragraphs = 0
    processed_pages = 0

    # iterparse for memory-safe streaming; watch 'end' on page elements
    context = ET.iterparse(xml_path, events=("end",))
    for event, elem in context:
        # detect page end
        if elem.tag == WIKI_NS + "page":
            # extract page id, title, namespace, and the latest revision/text
            page_id = None
            title = None
            ns = None
            text_blob = None

            # child traversal (small & bounded)
            for child in elem:
                tag = child.tag
                if tag == WIKI_NS + "title":
                    title = (child.text or "").strip()
                elif tag == WIKI_NS + "id" and page_id is None:
                    page_id = (child.text or "").strip()
                elif tag == WIKI_NS + "ns":
                    ns = (child.text or "").strip()
                elif tag == WIKI_NS + "revision":
                    # find text inside revision (revision can contain timestamp, contributor, text etc)
                    for rchild in child:
                        if rchild.tag == WIKI_NS + "text":
                            # text may be None or a huge string
                            text_blob = rchild.text or ""
                            break

            # If no text, skip
            if text_blob:
                paragraphs = split_into_paragraphs(text_blob)
                for idx, p in enumerate(paragraphs):
                    rows_buffer.append((page_id or "", title or "", ns or "", idx, p, os.path.basename(xml_path)))
                    total_paragraphs += 1

                    if len(rows_buffer) >= BATCH_SIZE:
                        batch_insert(conn, rows_buffer)
                        rows_buffer.clear()

            processed_pages += 1
            # Free memory for processed element: remove from tree
            elem.clear()
            # also clean up parent references to avoid memory growth
            while elem.getprevious() is not None:
                del elem.getparent()[0]

            # periodical logging
            if processed_pages % 1000 == 0:
                elapsed = time.time() - start_time
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] pages={processed_pages:,} paragraphs={total_paragraphs:,} elapsed={elapsed:.0f}s")
                sys.stdout.flush()

    # final flush
    if rows_buffer:
        batch_insert(conn, rows_buffer)
        rows_buffer.clear()

    conn.close()
    total_time = time.time() - start_time
    print(f"Done. pages={processed_pages:,} total_paragraphs={total_paragraphs:,} time={total_time:.0f}s")

# --------------------
# run
# --------------------
if __name__ == "__main__":
    if not os.path.exists(XML_PATH):
        print("XML file not found:", XML_PATH)
        sys.exit(2)
    stream_wiki_to_snowflake(XML_PATH)
