{{
    config(materialized='table')
}}

WITH ranked AS (
    SELECT
        genre,
        metric_date,
        track_name,
        artists,
        popularity,
        danceability,
        energy,
        valence,
        ROW_NUMBER() OVER (
            PARTITION BY genre, metric_date
            ORDER BY popularity DESC
        ) AS rank
    FROM {{ ref('stg_spotify_tracks') }}
)
SELECT * FROM ranked WHERE rank <= 10