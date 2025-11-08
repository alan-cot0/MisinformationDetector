from main import realtimeSTT
from snowflake_connector_and_similarity_querying import query_misinfo
from gemini_TRUTH_ANALYZER import gemini_fact_check
from wiki_paragraph_data_inserter import some_wiki_function  # <-- only if you end up needing it here later

for text_chunk in realtimeSTT():
    wiki_results = query_misinfo(text_chunk)
    verdict = gemini_fact_check(
        text_chunk,
        [r["paragraph_text"] for r in wiki_results]
    )
    
    print("USER SAID:", text_chunk)
    print("AI VERDICT:", verdict)
