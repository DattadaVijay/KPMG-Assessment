-- 1. Swimmer Rankings View
CREATE OR REPLACE VIEW vw_swimmer_rankings AS
WITH RankedPerformances AS (
    SELECT 
        ds.name AS swimmer_name,
        de.stroke,
        fp.time_seconds,
        RANK() OVER (PARTITION BY ds.swimmer_sk, de.stroke ORDER BY fp.time_seconds) as stroke_rank
    FROM fact_swimming_performance fp
    JOIN dim_swimmer ds ON fp.swimmer_sk = ds.swimmer_sk
    JOIN dim_event de ON fp.event_sk = de.event_sk
)
SELECT 
    swimmer_name,
    stroke,
    COUNT(*) FILTER (WHERE stroke_rank = 1) as rank_1_count,
    COUNT(*) FILTER (WHERE stroke_rank = 2) as rank_2_count,
    COUNT(*) FILTER (WHERE stroke_rank = 3) as rank_3_count,
    COUNT(*) FILTER (WHERE stroke_rank = 4) as rank_4_count,
    COUNT(*) FILTER (WHERE stroke_rank = 5) as rank_5_count
FROM RankedPerformances
GROUP BY swimmer_name, stroke;

-- 2. Club Performance by Region View
CREATE OR REPLACE VIEW vw_club_region_performance AS
WITH ClubPerformances AS (
    SELECT 
        ds.nationality as country,
        'Club data not available' as club_name,
        de.stroke,
        COUNT(DISTINCT ds.swimmer_sk) as swimmer_count
    FROM fact_swimming_performance fp
    JOIN dim_swimmer ds ON fp.swimmer_sk = ds.swimmer_sk
    JOIN dim_event de ON fp.event_sk = de.event_sk
    GROUP BY ds.nationality, de.stroke
),
ClubRankings AS (
    SELECT 
        country,
        club_name,
        MAX(CASE WHEN stroke = 'Butterfly' THEN swimmer_count END) as butterfly_count,
        MAX(CASE WHEN stroke = 'Freestyle' THEN swimmer_count END) as freestyle_count,
        MAX(CASE WHEN stroke = 'Backstroke' THEN swimmer_count END) as back_count,
        MAX(CASE WHEN stroke = 'Breaststroke' THEN swimmer_count END) as breast_count
    FROM ClubPerformances
    GROUP BY country, club_name
)
SELECT 
    cr.country,
    cr.club_name,
    COALESCE(cr.butterfly_count, 0) as butterfly_count,
    COALESCE(cr.freestyle_count, 0) as freestyle_count,
    COALESCE(cr.back_count, 0) as back_count,
    COALESCE(cr.breast_count, 0) as breast_count
FROM ClubRankings cr
ORDER BY country, 
    butterfly_count + freestyle_count + back_count + breast_count DESC;

-- 3. Swimmer Demographics View
CREATE OR REPLACE VIEW vw_swimmer_demographics AS
SELECT 
    nationality as swimmer_citizenship,
    COUNT(*) as total_swimmers,
    1 as birth_countries_count,
    nationality as birth_countries
FROM dim_swimmer
GROUP BY nationality
ORDER BY total_swimmers DESC; 

CREATE OR REPLACE VIEW vw_olympic_qualified_swimmers AS WITH qualified_swimmers AS (SELECT DISTINCT q.name, q.nationality, q.age, q.age_group, q.stroke, c.olympic_cut, COALESCE(r.rank_1_count, 0) as rank_1_count, q.total_competitions, q.qualifying_competitions FROM vw_olympic_qualifications q JOIN vw_olympic_cuts c ON q.name = c.name AND q.stroke = c.stroke LEFT JOIN vw_last_five_rankings r ON q.name = r.name AND q.stroke = r.stroke WHERE c.olympic_cut IN ('A Cut', 'B Cut') AND q.age >= 12 AND q.total_competitions >= 50 AND q.qualifying_competitions >= 5) SELECT * FROM qualified_swimmers ORDER BY CASE olympic_cut WHEN 'A Cut' THEN 1 WHEN 'B Cut' THEN 2 ELSE 3 END, stroke, rank_1_count DESC, name;

SELECT olympic_cut, stroke, COUNT(*) as qualified_count FROM vw_olympic_qualified_swimmers GROUP BY olympic_cut, stroke ORDER BY olympic_cut, stroke;

CREATE OR REPLACE VIEW vw_last_five_rankings AS WITH LastFiveCompetitions AS (SELECT s.name, s.nationality, e.stroke, sp.overall_rank, sp.competition_sk, ROW_NUMBER() OVER (PARTITION BY s.swimmer_sk, e.stroke ORDER BY c.competition_date DESC) as comp_order FROM dim_swimmer s JOIN fact_swimming_performance sp ON s.swimmer_sk = sp.swimmer_sk JOIN dim_event e ON sp.event_sk = e.event_sk JOIN dim_competition c ON sp.competition_sk = c.competition_sk WHERE s.is_current = true) SELECT name, nationality, stroke, COUNT(CASE WHEN overall_rank = 1 THEN 1 END) as rank_1_count FROM LastFiveCompetitions WHERE comp_order <= 5 GROUP BY name, nationality, stroke HAVING COUNT(CASE WHEN overall_rank = 1 THEN 1 END) >= 3 ORDER BY name, stroke;

CREATE OR REPLACE VIEW vw_olympic_cuts AS SELECT s.name, s.nationality, e.stroke, MIN(sp.time_seconds) as best_time, CASE WHEN MIN(sp.time_seconds) <= 25 THEN 'A Cut' WHEN MIN(sp.time_seconds) <= 30 THEN 'B Cut' ELSE 'Not Qualified' END as olympic_cut FROM dim_swimmer s JOIN fact_swimming_performance sp ON s.swimmer_sk = sp.swimmer_sk JOIN dim_event e ON sp.event_sk = e.event_sk WHERE s.is_current = true GROUP BY s.name, s.nationality, e.stroke ORDER BY e.stroke, best_time;

CREATE OR REPLACE VIEW vw_olympic_qualifications AS WITH SwimmerQualifications AS (SELECT s.swimmer_sk, s.name, s.nationality, s.birth_date, EXTRACT(YEAR FROM AGE(CURRENT_DATE, s.birth_date)) as age, CASE WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, s.birth_date)) BETWEEN 12 AND 14 THEN '12-14' WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, s.birth_date)) BETWEEN 15 AND 17 THEN '15-17' ELSE '17+' END as age_group, e.stroke, COUNT(DISTINCT sp.competition_sk) as total_competitions, COUNT(DISTINCT CASE WHEN sp.time_seconds BETWEEN 22 AND 50 THEN sp.competition_sk END) as qualifying_competitions, COUNT(DISTINCT CASE WHEN sp.overall_rank BETWEEN 1 AND 5 THEN sp.competition_sk END) as ranked_competitions FROM dim_swimmer s JOIN fact_swimming_performance sp ON s.swimmer_sk = sp.swimmer_sk JOIN dim_event e ON sp.event_sk = e.event_sk JOIN dim_competition c ON sp.competition_sk = c.competition_sk WHERE s.is_current = true AND EXTRACT(YEAR FROM AGE(CURRENT_DATE, s.birth_date)) >= 12 GROUP BY s.swimmer_sk, s.name, s.nationality, s.birth_date, e.stroke HAVING COUNT(DISTINCT sp.competition_sk) >= 50) SELECT sq.name, sq.nationality, sq.age, sq.age_group, sq.stroke, sq.total_competitions, sq.qualifying_competitions, sq.ranked_competitions FROM SwimmerQualifications sq WHERE sq.qualifying_competitions >= 5 ORDER BY sq.name, sq.stroke;