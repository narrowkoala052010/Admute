-- Migration 002: Add Variant Families (V5)
-- Safely adds columns to support Master/Child ad relationships and pending reviews.

-- 1. Upgrade the ads table to support Master/Child linking
ALTER TABLE ads ADD COLUMN parent_ad_id INTEGER REFERENCES ads(id) ON DELETE SET NULL;

-- 2. Upgrade the recordings table to support the "Review Links" UI workflow
ALTER TABLE recordings ADD COLUMN pending_link_ad_id INTEGER REFERENCES ads(id) ON DELETE SET NULL;