{{
    config(materialized='table')
}}

SELECT
    artists,
    metric_date,
    COUNT(DISTINCT track_id)        AS total_tracks,
    COUNT(DISTINCT genre)           AS genres_count,
    ROUND(AVG(popularity), 1)       AS avg_popularity,
    MAX(popularity)                 AS max_popularity,
    ROUND(AVG(danceability), 3)     AS avg_danceability,
    ROUND(AVG(energy), 3)           AS avg_energy,

    -- Tendencia de popularidad vs mes anterior
    LAG(ROUND(AVG(popularity), 1)) OVER (
        PARTITION BY artists
        ORDER BY metric_date
    ) AS avg_popularity_prev_month,

    ROUND(AVG(popularity), 1) - LAG(ROUND(AVG(popularity), 1)) OVER (
        PARTITION BY artists
        ORDER BY metric_date
    ) AS popularity_change

FROM {{ ref('stg_spotify_tracks') }}
GROUP BY artists, metric_date
ORDER BY artists, metric_date