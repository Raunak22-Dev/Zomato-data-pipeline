import os
import json
import snowflake.connector
from dotenv import load_dotenv
from openai import OpenAI


OLLAMA_BASE_URL = "http://host.docker.internal:11434/v1"

MODEL = "gemma4:e4b"

SAMPLE_N = 5
TOPICS = ["food quality", "delivery", "service", "packaging", "pricing", "other"]

load_dotenv()

client = OpenAI(
    base_url=OLLAMA_BASE_URL,
    api_key="ollama",  # placeholder — Ollama doesn't check this
)

SYSTEM_PROMPT = f"""You classify customer reviews for a food delivery app.

For the review you are given, return:
- sentiment_label: positive, negative, or neutral
- sentiment_score: a number between -1.0 and 1.0
- topic: one of {TOPICS}
- key_issue: a short phrase of 6 words or less that describes the main issue in the review. If there is no issue, return null.

Reply with JSON only, in exactly this format, no markdown fences or extra text:
{{
    "sentiment_label": "<sentiment_label>",
    "sentiment_score": <sentiment_score>,
    "topic": "<topic>",
    "key_issue": "<key_issue>"
}}
"""


def get_connection():
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )
    return conn


def create_output_table(cursor):
    cursor.execute("CREATE SCHEMA IF NOT EXISTS ZOMATO.AI")
    cursor.execute("""
CREATE TABLE IF NOT EXISTS ZOMATO.AI.REVIEW_ENRICHED (
    REVIEW_ID STRING,
    SENTIMENT_LABEL STRING,
    SENTIMENT_SCORE FLOAT,
    TOPIC STRING,
    KEY_ISSUE STRING,
    MODEL STRING,
    ENRICHED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
)
""")


def get_reviews_to_enrich(cursor, sample_n):
    cursor.execute(f"""
        SELECT REVIEW_ID, COMMENT
        FROM ZOMATO.RAW.REVIEWS
        WHERE REVIEW_ID NOT IN (SELECT REVIEW_ID FROM ZOMATO.AI.REVIEWS_ENRICHED)
        LIMIT {sample_n}
    """)
    return cursor.fetchall()


def _extract_json(text):
    """Local models often wrap JSON in ```json fences even when asked
    not to — strip those before parsing so json.loads doesn't choke."""
    text = text.strip()
    if text.startswith("```"):
        text = text.strip("`")
        if text.lower().startswith("json"):
            text = text[4:]
    return text.strip()


def classify_review(comment):
    response = client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": comment},
        ],
        temperature=0,
        response_format={"type": "json_object"},
    )
    answer = _extract_json(response.choices[0].message.content)
    return json.loads(answer)


def save_results(cursor, results):
    if not results:
        print(
            "No results to save — every review either failed classification or the list is empty."
        )
        return
    print(f"Inserting {len(results)} row(s) into REVIEWS_ENRICHED...")
    try:
        cursor.executemany(
            """
            INSERT INTO ZOMATO.AI.REVIEWS_ENRICHED (
                REVIEW_ID, SENTIMENT_LABEL, SENTIMENT_SCORE, TOPIC, KEY_ISSUE, MODEL
            ) VALUES (%(review_id)s, %(sentiment_label)s, %(sentiment_score)s, %(topic)s, %(key_issue)s, %(model)s)
            """,
            results,
        )
        print(f"Inserted {cursor.rowcount} row(s).")
    except snowflake.connector.Error as e:
        print(f"Insert failed: {e}")
        raise


def main():
    conn = get_connection()
    cursor = conn.cursor()

    create_output_table(cursor)
    reviews = get_reviews_to_enrich(cursor, SAMPLE_N)

    if len(reviews) == 0:
        print("No new reviews to enrich.")
        cursor.close()
        conn.close()
        return
    print(f"Enriching {len(reviews)} reviews...")

    results = []
    for review_id, comment in reviews:
        try:
            classification = classify_review(comment)
            results.append(
                {
                    "review_id": review_id,
                    "sentiment_label": classification["sentiment_label"],
                    "sentiment_score": classification["sentiment_score"],
                    "topic": classification["topic"],
                    "key_issue": classification["key_issue"],
                    "model": MODEL,
                }
            )
            print(f"Classified review {review_id}: {classification}")
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as e:
            print(f"Error classifying review {review_id}: {e}")

    save_results(cursor, results)
    conn.commit()
    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
