-- CREATE INDEX ind_dr_pisadomains ON oat.delivery_results USING gin (to_tsvector('english', login || ' ' || test_qti_id));
-- CREATE INDEX ind_dr_pisadomains ON oat.delivery_results USING gin (login gin_trgm_ops, test_qti_id gin_trgm_ops);
-- CREATE INDEX ind_covering on oat.delivery_results (login, test_qti_id) INCLUDE (raw_data);

-- CREATE INDEX ind_dr_pisadomains ON oat.delivery_results USING gin (login gin_trgm_ops, test_qti_id gin_trgm_ops, raw_data jsonb_path_ops);
-- CREATE INDEX ind_dr_pisadomains ON oat.delivery_results USING gin (login gin_trgm_ops, test_qti_id gin_trgm_ops, test_qti_label gin_trgm_ops);
-- CREATE INDEX ind_dr_pisadomains ON oat.delivery_results USING gin (login gin_trgm_ops, raw_data @> '{"metadata": ['PISA25 Domains']}' jsonb_path_ops);

-- DROP INDEX IF EXISTS oat.ind_dr_pisadomains;

-- SELECT row_id, login
-- FROM oat.delivery_results_check
-- WHERE raw_data->'metadata'->>'PISA25 Domains' = 'https://www.oecd.org/FLA'
-- AND (LENGTH(login) = 11 OR LENGTH(login) = 12)
-- AND (login LIKE '1%' OR login LIKE '2%' OR login LIKE '5%' OR login LIKE 'd%')
-- AND (login NOT LIKE '____9999%')
-- AND (login NOT LIKE '____A9999%')
-- AND (login NOT LIKE '%demo%');

-- CREATE INDEX ind_dr_ginlogin ON oat.delivery_results USING gin (login gin_trgm_ops);

-- SELECT
--     login, test_qti_id, delivery_id, raw_data
-- FROM oat.delivery_results
-- WHERE test_qti_id ~ '^FLA\-.*'
-- AND login ~ '(?<=^.{1})196'
-- AND login ~ '^[125].*'
-- AND login ~ '^((?!9999).)*$'
-- AND login ~ '^((?!demo).)*$'
-- AND login = '11960044002';

-- SELECT
--     login, test_qti_id, raw_data
-- FROM oat.delivery_results
-- WHERE test_qti_id ~ '^FLA\-.*'
-- AND login ~ '(?<=^.{1})056'
-- AND login ~ '^[125].*'
-- AND login ~ '^((?!9999).)*$'
-- AND login ~ '^((?!demo).)*$'
-- LIMIT 10;

-- SELECT DISTINCT login FROM oat.delivery_results;

-- TRUNCATE TABLE maple.isot_table;

-- WITH tab as (
--     SELECT
--         login, regexp_substr(login,'\d+') username, delivery_id, raw_data -> 'sessionEndTime' ->> 0 sessionEndtime
--     FROM oat.delivery_results
-- ) SELECT DISTINCT login, username, delivery_id, sessionEndtime FROM tab 
--     WHERE length(username) >= 11
--     AND username ~ '^[125]826'
--     LIMIT 10;

-- SELECT
--     s.delivery_execution_id, s.delivery_id, s.last_update_date, s.login,
--     s.test_qti_id, s.test_qti_label, s.test_qti_title, s.raw_data, s.raw_data->'metadata'->>'PISA25 Languages' as language
-- FROM oat.delivery_results as s
-- INNER JOIN (
--     SELECT p.username, p.login FROM maple.maple_student_post_val as p
-- ) as p
-- ON p.login = s.login
-- WHERE p.login IS NOT NULL AND p.username ~ '^[125]040' AND s.test_qti_id ~ '^FLA\-.*';

SELECT
    b.login,
    b.itemId,
    b.unit_id,
    jsonb_object_keys(b.responses) as resp_cat
FROM (
    SELECT
        t.login, t.itemId,
        t.raw_data->'items'->itemId->>'qtiLabel' as unit_id,
        t.raw_data->'items'->itemId->'responses' as responses
    FROM (
        SELECT 
            login,
            regexp_substr(login,'\d+') as username,
            raw_data,
            test_qti_id,
            jsonb_object_keys(raw_data->'items') as itemId
        FROM oat.delivery_results
        WHERE test_qti_id ~ '^FLA\-.*'
    ) AS t
    WHERE t.username ~ '^[125]196'
) AS b
WHERE jsonb_typeof(b.responses) = 'object'
AND b.login IN (
    SELECT p.login FROM maple.maple_student_post_val as p WHERE p.username ~ '^[125]196'
);

-- SELECT login, test_qti_id, delivery_id, raw_data
-- FROM oat.delivery_results
-- WHERE login = '11960001013';

-- SELECT login,
--     jsonb_object_keys(raw_data->'items') as qtiId,
--     raw_data->'items'->qtiId as qtiIds
-- FROM oat.delivery_results
--     WHERE login = '11960044002';

-- SELECT
--     s.delivery_execution_id, s.delivery_id, s.last_update_date, s.login,
--     s.test_qti_id, s.test_qti_label, s.test_qti_title, s.raw_data,
--     s.raw_data->'metadata'->>'PISA25 Languages' as language
-- FROM oat.delivery_results as s 
-- WHERE s.login IN (
--     SELECT p.login FROM maple.maple_student_post_val as p WHERE p.username ~ '^[125]196'
-- ) AND s.test_qti_id ~ '^FLA\-.*' AND login = '11960044002';

-- SELECT
--     delivery_execution_id, delivery_id, last_update_date, login,
--     test_qti_id, test_qti_label, test_qti_title, raw_data,
--     raw_data->'metadata'->>'PISA25 Languages' as language
-- FROM oat.delivery_results
-- WHERE test_qti_id ~ '^FLA'
-- AND login = '11960044002';

-- SELECT
--     s.delivery_execution_id, s.delivery_id, s.last_update_date, s.login,
--     s.test_qti_id, s.test_qti_label, s.test_qti_title, s.raw_data,
--     s.raw_data->'metadata'->>'PISA25 Languages' as language
-- FROM oat.delivery_results as s 
-- WHERE s.login IN (
--     SELECT p.login FROM maple.maple_student_post_val as p WHERE p.username ~ '^[125]196'
-- ) AND s.test_qti_id ~ '^FLA\-.*' AND login = '11960044002';

-- SELECT
--     s.delivery_execution_id, s.delivery_id, s.last_update_date, s.login,
--     s.test_qti_id, s.test_qti_label, s.test_qti_title, s.raw_data,
--     s.raw_data->'metadata'->>'PISA25 Languages' as language
-- FROM oat.delivery_results as s 
-- WHERE s.login IN (
--     SELECT p.login FROM maple.maple_student_post_val as p WHERE p.username ~ '^[125]196'
-- ) AND s.test_qti_id ~ '^FLA\-.*' AND login = '11960044002';

-- SELECT p.login as login, p.username as username, s.delivery_execution_id, s.delivery_id,
--         s.test_qti_id, s.test_qti_label, s.test_qti_title, s.raw_data,
--         s.raw_data->'metadata'->>'PISA25 Languages' as language
-- FROM maple.maple_student_post_val as p
-- INNER JOIN (
--     SELECT
--         row_id, delivery_execution_id, delivery_id, last_update_date, login,
--         test_qti_id, test_qti_label, test_qti_title, raw_data, raw_data->'metadata'->>'PISA25 Languages' as language
--     FROM oat.delivery_results as s
-- ) as s
-- ON p.login = s.login
-- WHERE p.login IS NOT NULL AND p.username ~ '^[125]040' AND s.test_qti_id ~ '^FLA\-.*';

-- SELECT
--     login, test_qti_id, raw_data
-- FROM oat.delivery_results
-- WHERE test_qti_id ~ '^FLA.*'
-- AND login ~ '(?<=^.{1})056'
-- AND login ~ '^[125].*'
-- AND login ~ '^((?!9999).)*$'
-- AND login ~ '^((?!demo).)*$';

-- SELECT login, test_qti_id, raw_data
-- FROM oat.delivery_results
-- WHERE
--     to_tsvector('english', login || ' ' || test_qti_id) @@
--     to_tsquery('english','^SA1:*');

-- AND raw_data @> '{"metadata": {"PISA25 Domains": "https://www.oecd.org/SCI"}}';

-- SELECT delivery_execution_id, login, test_qti_label, raw_data
-- FROM oat.delivery_results
-- WHERE login like '%320%';
-- AND (LENGTH(login) = 11 OR LENGTH(login) = 12);
-- AND (login LIKE '1%' OR login LIKE '2%' OR login LIKE '5%' OR login LIKE 'd%')
-- AND (login NOT LIKE '____9999%')
-- AND (login NOT LIKE '____A9999%')
-- AND (login NOT LIKE '%demo%');