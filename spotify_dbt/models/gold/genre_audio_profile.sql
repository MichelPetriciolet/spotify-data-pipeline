{{
    config(materialized='table')
}}

SELECT
    genre,
    metric_date,
    COUNT(DISTINCT track_id)        AS total_tracks,
    ROUND(AVG(popularity), 1)       AS avg_popularity,
    ROUND(AVG(danceability), 3)     AS avg_danceability,
    ROUND(AVG(energy), 3)           AS avg_energy,
    ROUND(AVG(valence), 3)          AS avg_valence,
    ROUND(AVG(tempo_bpm), 1)        AS avg_tempo,
    ROUND(AVG(acousticness), 3)     AS avg_acousticness,
    ROUND(AVG(loudness), 2)         AS avg_loudness,
    COUNT(CASE WHEN is_explicit THEN 1 END) AS explicit_tracks
FROM {{ ref('stg_spotify_tracks') }}
GROUP BY genre, metric_date
ORDER BY metric_date, avg_popularity DESC