"""
AdMute Light
Copyright (c) 2026 Carlos C. (narrowkoala052010)

Part of the AdMute Project.
Licensed under the MIT License — see LICENSE for details.
"""

"""
Shared time-coherence scoring algorithm used by both the MatchEngine
L3 fallback and the API ingest variant pre-check.
"""


import sqlite3
from collections import Counter, defaultdict


def query_vault(conn: sqlite3.Connection,
                hash_values: list) -> list:
    """
    Find all hashes in the vault that match any of the query hashes.
    Only returns rows for active ads.
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
        JOIN    ads    a ON h.ad_id = a.id
        WHERE   a.is_active = 1
          AND   h.hash_value IN ({placeholders})
    """
    return conn.execute(sql, hash_values).fetchall()


def time_coherence_score(query_map: dict,
                         db_rows:   list) -> dict:
    """
    Compute a time-coherence score for each candidate ad.

    Builds a histogram of (db_offset - query_offset) deltas for every
    hash that appears in both the query and the vault.  The height of
    the tallest bin is the score — a genuine match produces a sharp
    spike; noise produces a flat distribution.

    Returns:
        {ad_id: (score, ad_name, duration_seconds, peak_delta)}

    peak_delta is the modal time offset in frames — useful for
    diagnosing alignment issues during ingest tuning.
    """
    ad_info:   dict = {}
    ad_deltas: dict = defaultdict(list)

    for row in db_rows:
        h_val     = row["hash_value"]
        db_offset = row["time_offset"]
        ad_id     = row["ad_id"]

        if h_val not in query_map:
            continue

        delta = db_offset - query_map[h_val]
        ad_deltas[ad_id].append(delta)
        ad_info[ad_id] = (row["name"], row["duration_seconds"])

    scores: dict = {}
    for ad_id, deltas in ad_deltas.items():
        counter    = Counter(deltas)
        peak_delta, peak = counter.most_common(1)[0]
        name, dur  = ad_info[ad_id]
        scores[ad_id] = (peak, name, dur, peak_delta)

    return scores