With source as (

    SELECT * FROM {{source ('bronze','spotify_tracks') }}
),
deduped as (
    Select *, 
        ROW_NUMBER() OVER (
            PARTITION BY track_id, metric_date
            ORDER BY _loaded_at DESC
        ) AS rn
    FROM source
    WHERE track_id IS NOT NULL
        AND metric_date IS NOT NULL
),
cleaned AS (
    SELECT
        track_id,
        TRIM(artists)                        AS artists,
        TRIM(album_name)                     AS album_name,
        TRIM(track_name)                     AS track_name,
        popularity::INT                      AS popularity,
        ROUND(duration_ms / 60000.0, 2)     AS duration_min,
        explicit::BOOLEAN                    AS is_explicit,
        danceability,
        energy,
        key::INT                             AS musical_key,
        loudness,
        CASE WHEN mode = 1 THEN 'Major'
             ELSE 'Minor' END                AS musical_mode,
        speechiness,
        acousticness,
        instrumentalness,
        liveness,
        valence,
        ROUND(tempo, 1)                      AS tempo_bpm,
        time_signature::INT                  AS time_signature,
        LOWER(TRIM(track_genre))             AS genre,
        metric_date,
        year,
        month,
        day,
        _loaded_at
    FROM deduped
    WHERE rn = 1
      AND popularity IS NOT NULL
)
SELECT * FROM cleaned