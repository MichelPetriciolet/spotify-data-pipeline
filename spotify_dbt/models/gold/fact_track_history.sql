{{
    config(
        materialized='incremental',
        unique_key=['track_id','metric_date'],
        incremental_strategy='merge'
    )
}}

SELECT
    track_id,
    track_name,
    artists,
    album_name,
    genre,
    metric_date,
    popularity,
    duration_min,
    is_explicit,
    danceability,
    energy,
    valence,
    tempo_bpm,
    acousticness,
    instrumentalness,
    liveness,
    speechiness,
    loudness,
    musical_key,
    musical_mode,
    year,
    month,
    day,
    _loaded_at,
    CASE
        WHEN popularity >= 70 THEN 'high'
        WHEN popularity >= 40 THEN 'medium'
        ELSE 'low'
    END AS popularity_tier

FROM {{ ref('stg_spotify_tracks') }}

{%- if is_incremental() %}
WHERE metric_date > (SELECT MAX(metric_date) FROM {{ this }})
{%- endif %}