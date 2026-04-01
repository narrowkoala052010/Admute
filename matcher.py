"""
AdMute v4 — Shared Matcher Logic
Contains the time-coherence scoring algorithm used by both
the live MatchEngine and the API ingestion pre-checks.
"""

import sqlite3
from collections import Counter, defaultdict

def query_vault(conn: sqlite3.Connection, hash_values: list[int]) -> list[sqlite3.Row]:
    """
    Find all hashes in the vault that match any of our query hashes.
    """
    if not hash_values:
        return []

    placeholders = ",".join("?" * len(hash_values))
    sql = f"""
        SELECT  h.hash_value,
                h.time_offset,
                h.ad_id,
                a.name,
                a.duration_seconds
        FROM    hashes h
        JOIN    ads a ON h.ad_id = a.id
        WHERE   a.is_active = 1
          AND   h.hash_value IN ({placeholders})
    """
    return conn.execute(sql, hash_values).fetchall()


def time_coherence_score(query_map: dict[int, int], db_rows: list[sqlite3.Row]) -> dict[int, tuple[int, str, float]]:
    """
    Compute the time-coherence score using a histogram of time offsets.
    Returns: {ad_id: (score, ad_name, duration_seconds)}
    """
    ad_info:   dict[int, tuple[str, float]] = {}
    ad_deltas: dict[int, list[int]]         = defaultdict(list)

    for row in db_rows:
        h_val     = row["hash_value"]
        db_offset = row["time_offset"]
        ad_id     = row["ad_id"]

        if h_val not in query_map:
            continue

        delta = db_offset - query_map[h_val]
        ad_deltas[ad_id].append(delta)
        ad_info[ad_id] = (row["name"], row["duration_seconds"])

    scores: dict[int, tuple[int, str, float]] = {}
    for ad_id, deltas in ad_deltas.items():
        counter = Counter(deltas)
        most_common = counter.most_common(1)[0]
        peak = most_common[1]       # How many hashes hit this offset
        peak_delta = most_common[0] # The physical time offset in milliseconds

        name, dur = ad_info[ad_id]
        scores[ad_id] = (peak, name, dur, peak_delta) # NEW: Returning peak_delta

    return scores