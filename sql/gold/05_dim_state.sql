-- dim_state: US states, DC and territories from a reference seed (sql/seeds/us_states.csv).
SELECT xxhash64(state_code) AS state_key, state_code, state_name, census_region FROM seed_us_states
UNION ALL SELECT -1, 'UNKNOWN', 'Unknown', 'Unknown'
