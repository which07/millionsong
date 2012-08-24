/**
 * top_density_songs: Find the top 50 most dense songs in the million song dataset.
 *
 * Required parameters:
 *
 * -param OUTPUT_PATH Output path for script data (e.g. s3n://my-output-bucket/millionsong)
 */

-- User-Defined Functions (UDFs)
REGISTER '../udfs/python/millionsong.py' USING streaming_python AS millionsong;

-- Load up the million song dataset from S3 (see data spec at: http://bit.ly/vOBKPe)
songs = LOAD 's3n://tbmmsd/*.tsv.*' USING PigStorage('\t') AS (
     track_id:chararray, analysis_sample_rate:chararray, artist_7digitalid:chararray,
     artist_familiarity:chararray, artist_hotness:double, artist_id:chararray, artist_latitude:chararray, 
     artist_location:chararray, artist_longitude:chararray, artist_mbid:chararray, artist_mbtags:chararray, 
     artist_mbtags_count:chararray, artist_name:chararray, artist_playmeid:chararray, artist_terms:chararray, 
     artist_terms_freq:chararray, artist_terms_weight:chararray, audio_md5:chararray, bars_confidence:chararray, 
     bars_start:chararray, beats_confidence:chararray, beats_start:chararray, danceability:double, 
     duration:float, end_of_fade_in:chararray, energy:chararray, key:chararray, key_confidence:chararray, 
     loudness:chararray, mode:chararray, mode_confidence:chararray, release:chararray, 
     release_7digitalid:chararray, sections_confidence:chararray, sections_start:chararray, 
     segments_confidence:chararray, segments_loudness_max:chararray, segments_loudness_max_time:chararray, 
     segments_loudness_max_start:chararray, segments_pitches:chararray, segments_start:chararray, 
     segments_timbre:chararray, similar_artists:chararray, song_hotness:chararray, song_id:chararray, 
     start_of_fade_out:chararray, tatums_confidence:chararray, tatums_start:chararray, tempo:double, 
     time_signature:chararray, time_signature_confidence:chararray, title:chararray, track_7digitalid:chararray, 
     year:int );

-- Use FILTER to get only songs that have a duration
filtered_songs = FILTER songs BY duration > 0;

-- Use FOREACH to run calculations on every row.
-- Here, we calculate density (sounds per second) using 
-- the density function we wrote in python below
song_density = FOREACH filtered_songs 
              GENERATE artist_name, 
                       title,
                       millionsong.density(segments_start, duration);

-- Get the top 50 most dense songs
-- by using ORDER and then LIMIT
density_ordered = ORDER song_density BY density DESC;
top_density     = LIMIT density_ordered 50;

-- STORE the top 50 songs into S3
-- We use the pig 'rmf' command to remove any
-- existing results first
rmf $OUTPUT_PATH;
STORE top_density 
 INTO '$OUTPUT_PATH' 
USING PigStorage('\t');
