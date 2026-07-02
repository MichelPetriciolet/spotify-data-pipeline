{{
    config(materialized='table')
}}

SELECT
    track_id,
    track_name,
    artists,
    genre,
    metric_date,
    popularity,
    energy,
    danceability,
    valence,

    -- Valores del mes anterior
    LAG(popularity) OVER (
        PARTITION BY track_id ORDER BY metric_date
    ) AS popularity_prev_month,

    LAG(energy) OVER (
        PARTITION BY track_id ORDER BY metric_date
    ) AS energy_prev_month,

    -- Variación absoluta
    popularity - LAG(popularity) OVER (
        PARTITION BY track_id ORDER BY metric_date
    ) AS popularity_change,

    -- Tendencia
    CASE
        WHEN popularity > LAG(popularity) OVER (PARTITION BY track_id ORDER BY metric_date)
            THEN 'rising'
        WHEN popularity < LAG(popularity) OVER (PARTITION BY track_id ORDER BY metric_date)
            THEN 'falling'
        ELSE 'stable'
    END AS trend

FROM {{ ref('fact_track_history') }}
ORDER BY track_id, metric_date