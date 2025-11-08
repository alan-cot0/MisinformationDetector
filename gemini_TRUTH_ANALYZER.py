import os
import google.generativeai as genai

genai.configure(api_key=os.getenv("GEMINI_API_KEY"))

def gemini_fact_check(stt_text, wiki_paragraphs):
    # Construct prompt
    prompt = f"""
Fact-check the following statement: "{stt_text}"

Here are relevant facts from Wikipedia:
"""
    for i, para in enumerate(wiki_paragraphs, 1):
        prompt += f"{i}. {para}\n"

    prompt += """
Question: Based on the context above, is the statement TRUE, FALSE, or MISLEADING?
Provide a short explanation.
"""
    # Call Gemini API
    response = genai.chat.create(
        model="gemini-pro",
        messages=[{"role": "user", "content": prompt}]
    )
    
    verdict = response.last.message.content
    return verdict
