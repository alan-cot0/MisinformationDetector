# main2.py
import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from RealtimeSTT import AudioToTextRecorder  #  adapted STT class
from fastapi import FastAPI, UploadFile, File
import google.generativeai as genai
import re
import os
import sys
import time
import snowflake.connector
import xml.etree.ElementTree as ET
from account_snowflake_reach import user, password, account, DATABASE, SCHEMA
from account_snowflake_reacher import USER, PASSWORD, ACCOUNT, WAREHOUSE, DATABASE, SCHEMA, ROLE

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.websocket("/ws/audio")
async def websocket_audio_endpoint(websocket: WebSocket):
    await websocket.accept()
    print("Client connected")

    # Create an instance of the STT processor
    stt_processor = AudioToTextRecorder()  

    try:
        while True:
            audio_chunk = await websocket.receive_bytes()
            
            # Convert audio chunk -> text
            text = stt_processor.feed_audio(audio_chunk)
            # Run Gemini truth analysis
            #truth_score, message = analyze_truth(text)
            @app.post("/truth-check/")
            async def truth_check(text: str, wiki_paragraphs: list[str]):
                verdict = gemini_fact_check(text, wiki_paragraphs)
                return {"verdict": verdict}
            truth_score = truth_check(text)

            await websocket.send_json({
                "transcript": text,
                "truth_score": truth_score,
                #"message": message
            })

    except WebSocketDisconnect:
        print("Client disconnected")
        stt_processor.shutdown()

# DIVIDER

genai.configure(api_key="AIzaSyB44WWsiLt0jdGc1VurbV0ZVnzLSN5aDVY")

def _parse_verdict_text(text):
    # crude extractor: find TRUE / FALSE / MISLEADING and a short percent if present
    t = text.upper()
    score = None
    if "TRUE" in t:
        score = 0.9
    if "FALSE" in t:
        score = 0.1
    if "MISLEADING" in t or "PARTLY" in t:
        score = 0.5
    # look for an explicit percent
    m = re.search(r'(\d{1,3})\s*%', text)
    if m:
        try:
            score = float(m.group(1)) / 100.0
        except:
            pass
    return score

def gemini_fact_check(stt_text, wiki_paragraphs):
    prompt = f"""Fact-check the statement below as TRUE, FALSE, or MISLEADING and give a one-line numeric confidence score (0-100) plus a one-sentence explanation.

Statement:
\"\"\"{stt_text}\"\"\"

Relevant context (from Wikipedia):
"""
    for i, para in enumerate(wiki_paragraphs, 1):
        prompt += f"{i}. {para}\n"

    prompt += "\nPlease answer in this JSON format exactly:\n{\"verdict\": \"TRUE|FALSE|MISLEADING\", \"confidence\": 0-100, \"explanation\": \"...\"}\n"

    response = genai.chat.create(
        model="gemini-pro",
        messages=[{"role": "user", "content": prompt}],
        max_output_tokens=300
    )

    text = response.last.message.content.strip()

#need to figure out how to run this on an XML file.
    try:
        import json
        parsed = json.loads(text)
        confidence = parsed.get("confidence")
        if confidence is not None:
            score = float(confidence) / 100.0
        else:
            score = _parse_verdict_text(text)
        return {"truth_score": score, "message": parsed.get("explanation") if parsed.get("explanation") else text}
    except Exception:
        score = _parse_verdict_text(text)
        return {"truth_score": score, "message": text}

# DIVIDER 

"""
Streaming ingestion for enwiki-latest-pages-articles.xml -> Snowflake

Notes:
- Designed for very large XML dumps (streaming with iterparse).
- Adjust BATCH_SIZE for performance / memory tradeoffs.
- Minimal cleaning is applied. For better downstream retrieval you'll
  likely want to add wiki-markup cleaning (mwparserfromhell or wikiextractor)
  as a later step.
"""
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

# DIVIDER

import snowflake.connector

def query_misinfo(stt_text, top_n=3):
    """
    Process a single STT chunk in Snowflake and return top matching KB paragraphs.
    stt_text (str) is the text from RealtimeSTT
    top_n (int) is how many of the top most similar paragraphs to use for the truth comparison. It is set by default to three at the time of writing.
    This will return a list of dicts; each dict has paragraph_id, paragraph_text, similarity_score, source_url.
    """

    # Here we are going to connect to Snowflake
    ctx = snowflake.connector.connect(
        user=USER,
        password=PASSWORD,
        account=ACCOUNT,
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA,
        role=ROLE
    )
    cs = ctx.cursor()

    try:
        # Create temporary input table
        cs.execute("CREATE OR REPLACE TEMP TABLE stt_input (stt_id STRING, stt_text STRING)")

        # Insert the STT chunk (We are using RealtimeSTT for speech to text purposes.)
        cs.execute("INSERT INTO stt_input (stt_id, stt_text) VALUES (%s, %s)", ("chunk1", stt_text))

        # Embed the chunk into temp table
        cs.execute("""
            CREATE OR REPLACE TEMP TABLE stt_embedded AS
            SELECT
                stt_id,
                stt_text,
                AI_EMBED('snowflake-arctic-embed-l-v2.0', stt_text) AS embedding
            FROM stt_input
        """)

        # Query similarity against KB
        cs.execute(f"""
            SELECT
                kb.paragraph_id,
                kb.paragraph_text,
                VECTOR_COSINE_SIMILARITY(s.embedding, kb.embedding) AS similarity_score,
                kb.source_url
            FROM stt_embedded s
            CROSS JOIN embedded_paragraphs kb
            ORDER BY similarity_score DESC
            LIMIT {top_n}
        """)

        results = []
        for row in cs.fetchall():
            results.append({
                "paragraph_id": row[0],
                "paragraph_text": row[1],
                "similarity_score": row[2],
                "source_url": row[3]
            })
        print("Connecting to Snowflake…")
        print("Fetched rows:", len(results))

        return results

    finally:
        cs.close()
        ctx.close()


# Example usage:
if __name__ == "__main__":
    stt_chunk = "COVID-19 is just a cold"
    top_matches = query_misinfo(stt_chunk)
    for m in top_matches:
        print(m)



