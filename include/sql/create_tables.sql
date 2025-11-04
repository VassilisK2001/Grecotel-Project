-- Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS {{ params.schema }};

-- Drop hotel table if it exists
DROP TABLE IF EXISTS {{ params.schema }}.{{ params.hotel_table }} CASCADE;

-- Create hotel table
CREATE TABLE IF NOT EXISTS {{ params.schema }}.{{ params.hotel_table }} (
    hotel_id TEXT PRIMARY KEY,
    hotel_name TEXT,
    hotel_rating REAL,
    n_reviews INTEGER,
    hotel_location TEXT
);

-- Drop review table if it exists
DROP TABLE IF EXISTS {{ params.schema }}.{{ params.review_table }} CASCADE;

-- Create review table
CREATE TABLE IF NOT EXISTS {{ params.schema }}.{{ params.review_table }} (
    review_id TEXT PRIMARY KEY,
    review_lang_original TEXT, 
    review_rating REAL, 
    review_text_en TEXT,
    travel_date DATE,
    review_published_date DATE,
    trip_type TEXT,
    reviewer_location TEXT,
    hotel_id TEXT REFERENCES {{ params.schema }}.{{ params.hotel_table }} 
);
