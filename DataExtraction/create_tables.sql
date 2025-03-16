DROP TABLE IF EXISTS dim_swimmer CASCADE;
DROP SEQUENCE IF EXISTS dim_swimmer_sk_seq;
CREATE SEQUENCE IF NOT EXISTS dim_swimmer_sk_seq;
CREATE TABLE dim_swimmer (
    swimmer_sk BIGINT PRIMARY KEY DEFAULT nextval('dim_swimmer_sk_seq'),
    swimmer_id VARCHAR(50) UNIQUE,
    name VARCHAR(255),
    birth_date DATE,
    gender VARCHAR(1),
    nationality VARCHAR(3),
    is_current BOOLEAN DEFAULT TRUE,  -- Added columns
    valid_from DATE DEFAULT CURRENT_DATE, -- Added columns
    valid_to DATE,                   -- Added columns
    version INTEGER DEFAULT 1,       -- Added columns
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Added columns
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- Added columns
);

DROP TABLE IF EXISTS dim_event CASCADE;
DROP SEQUENCE IF EXISTS dim_event_sk_seq;
CREATE SEQUENCE IF NOT EXISTS dim_event_sk_seq;
CREATE TABLE dim_event (
    event_sk BIGINT PRIMARY KEY DEFAULT nextval('dim_event_sk_seq'),
    distance INTEGER,
    stroke VARCHAR(50),
    course VARCHAR(50)
);

DROP TABLE IF EXISTS dim_competition CASCADE;
DROP SEQUENCE IF EXISTS dim_competition_sk_seq;
CREATE SEQUENCE IF NOT EXISTS dim_competition_sk_seq;
CREATE TABLE dim_competition (
    competition_sk BIGINT PRIMARY KEY DEFAULT nextval('dim_competition_sk_seq'),
    competition_id VARCHAR(50) UNIQUE,
    competition_date DATE,
    location_code VARCHAR(50),
    competition_name VARCHAR(255)
);

DROP TABLE IF EXISTS fact_swimming_performance CASCADE;
CREATE TABLE fact_swimming_performance (
    performance_id BIGINT PRIMARY KEY,
    swimmer_sk BIGINT REFERENCES dim_swimmer(swimmer_sk),
    event_sk BIGINT REFERENCES dim_event(event_sk),
    competition_sk BIGINT REFERENCES dim_competition(competition_sk),
    time_seconds DOUBLE PRECISION,
    overall_rank INTEGER,
    age INTEGER
);

DROP TABLE IF EXISTS fact_rankings CASCADE;
DROP SEQUENCE IF EXISTS fact_rankings_id_seq;
CREATE SEQUENCE IF NOT EXISTS fact_rankings_id_seq;
CREATE TABLE fact_rankings (
    ranking_id BIGINT PRIMARY KEY DEFAULT nextval('fact_rankings_id_seq'),
    swimmer_sk BIGINT REFERENCES dim_swimmer(swimmer_sk),
    event_sk BIGINT REFERENCES dim_event(event_sk),
    best_time DOUBLE PRECISION,
    best_rank INTEGER,
    UNIQUE (swimmer_sk, event_sk)
);