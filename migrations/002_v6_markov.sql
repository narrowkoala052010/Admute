-- AdMute v6: Markov Transition Matrix and Telemetry support

CREATE TABLE IF NOT EXISTS markov_transitions (
    source_ad_id INTEGER NOT NULL,
    target_ad_id INTEGER NOT NULL,
    transition_count INTEGER DEFAULT 1,
    last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source_ad_id, target_ad_id),
    FOREIGN KEY(source_ad_id) REFERENCES ads(id) ON DELETE CASCADE,
    FOREIGN KEY(target_ad_id) REFERENCES ads(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_markov_source ON markov_transitions(source_ad_id);