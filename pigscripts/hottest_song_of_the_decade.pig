/**
 * hottest_song_of_the_decade: Find the hottest songs from each decade!
 *
 * Required parameters:
 *
 * -param OUTPUT_PATH Output path for script data (e.g. s3n://my-output-bucket/hottest_song_of_the_decade)
 */

-- User-Defined Functions (UDFs)
REGISTER '../udfs/python/date_utils.py' USING streaming_python AS date_utils;

-- Macros
IMPORT '../macros/millionsong.pig';

-- Load up the million song dataset
-- using our ALL_SONGS() pig macro 
-- (we can substitute ONE_SONGS_FILE() to get a smaller dataset)
songs = ALL_SONGS();

-- Use FILTER to get only songs that have a year and a song_hotness
filtered = FILTER songs 
               BY year IS NOT NULL 
              AND year > 0
              AND song_hotness IS NOT NULL
              AND song_hotness > 0.0;

-- Convert the year into a decade using python
with_decade = FOREACH filtered
             GENERATE *, date_utils.decade(year) AS decade;

-- group the rows by decade, and find the hottest song
-- in each decade
grouped = GROUP with_decade BY decade;
top_song_by_decade = FOREACH grouped {
    -- order to put the hottest song in this group on top
    ordered = ORDER with_decade BY song_hotness DESC;
    
    -- grab only the hottest song
    top_song = LIMIT ordered 1;
    GENERATE group as decade, flatten(top_song);
};

-- order them by year to make it nice and pretty
ordered_by_year = ORDER top_song_by_decade BY year ASC;

-- grab just the fields we want to output
output_data = FOREACH ordered_by_year
             GENERATE decade, artist_name, title, year, song_hotness;

-- STORE the list of top songs by decade
-- We use the pig 'rmf' command to remove any
-- existing results first
rmf $OUTPUT_PATH;
STORE output_data 
 INTO '$OUTPUT_PATH' 
USING PigStorage('\t');
