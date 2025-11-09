# gemini_TRUTH_ANALYZER.py
import google.generativeai as genai
import re
#import pytest
import google.generativeai as genai


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

"""corpus = [
    "The capital of France is Paris.",
    "Dogs are commonly kept as pets.",
    "The Sun is a star.",
    "Neptune is the farthest planet in the Solar System.",
    "Cats chase laser pointers."
    "Paris serves as the administrative center of France."
    "Canines have been companions to humans for thousands of years."
    "Our star, the Sun, powers nearly all life on Earth."

]


import numpy as np

class MyVectorIndex:
    def __init__(self):
        self.docs = []
        self.vecs = []

    def embed(self, text):
        # use Gemini embedding model
        emb = genai.embed_content(
            model="models/text-embedding-004",
            content=text
        )["embedding"]
        return np.array(emb, dtype=float)

    def add(self, id, text):
        self.docs.append(text)
        self.vecs.append(self.embed(text))

    def search(self, query, k=1):
        qv = self.embed(query)
        sims = [ self.cosine(qv, v) for v in self.vecs ]
        top = np.argsort(sims)[::-1][:k]
        return [ self.docs[i] for i in top ]

    def score(self, query, text):
        return self.cosine(self.embed(query), self.embed(text))

    def cosine(self, a, b):
        return float(np.dot(a,b) / (np.linalg.norm(a)*np.linalg.norm(b)))


@pytest.fixture(scope="session")
def vector_index():
    # *** replace with your embedding + add to DB ***
    # (e.g. encode corpus + store in vector DB)
    index = MyVectorIndex()
    for i, text in enumerate(corpus):
        index.add(str(i), text)
    return index


def top_hit(index, query):
    # general helper to avoid repetition
    hits = index.search(query, k=1)
    return hits[0]  # return text string

def test_planet(vector_index):
    assert "Neptune" in top_hit(vector_index, "which planet is farthest from the sun?")

def test_capital(vector_index):
    assert "Paris" in top_hit(vector_index, "what is the capital of france?")

def test_dogs(vector_index):
    assert "Dogs" in top_hit(vector_index, "are dogs domestic animals?")

def test_sun(vector_index):
    assert "Sun" in top_hit(vector_index, "is our star the sun?")

def test_noise_low_similarity(vector_index):
    hit = top_hit(vector_index, "I like eating ramen noodles.")
    # ramen should NOT logically map to anything
    # so the best we can do is assert that the cosine similarity < threshold
    sim = vector_index.score("I like eating ramen noodles.", hit)
    assert sim < 0.35   # you can adjust threshold depending on model quality


"""