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
--     login, test_qti_id, SUBSTRING(login,5,4) as schid
-- FROM oat.delivery_results
-- WHERE test_qti_id ~ '^FLA\-.*'
-- AND login ~ '(?<=^.{1})056'
-- AND login ~ '^[125].*'
-- AND login ~ '^((?!9999).)*$'
-- AND login ~ '^((?!demo).)*$';

-- SELECT
--     login, test_qti_id, raw_data -> 'sessionEndTime' ->> 0 sessionEndtime, last_update_date,raw_data
-- FROM oat.delivery_results
-- WHERE test_qti_id ~ '^FLA\-.*'
-- AND login ~ '(?<=^.{5})380'
-- AND login ~ '^[125].*'
-- AND login ~ '^((?!9999).)*$'
-- AND login ~ '^((?!demo).)*$'
-- LIMIT 10;

-- WITH tab as (
--     SELECT
--         login, regexp_substr(login,'\d+') username, delivery_id, raw_data -> 'sessionEndTime' ->> 0 sessionEndtime
--     FROM oat.delivery_results
-- ) SELECT DISTINCT login, username, delivery_id, sessionEndtime FROM tab
--     WHERE length(username) >= 11
--     AND username ~ '^5440'
-- LIMIT 1000;


-- SELECT
--     delivery_execution_id, delivery_id, login, 
--     test_qti_id, last_update_date, raw_data
-- FROM oat.delivery_results
-- WHERE
--     test_qti_id IN (
--         'LDW1-LDW2', 'LDW1-LDW5', 'LDW2-LDW4', 'LDW2-LDW6',
--         'LDW3-LDW4', 'LDW3-LDW6', 'LDW4-LDW5', 'LDW4-LDW7',
--         'LDW5-LDW3', 'LDW5-LDW8', 'LDW6-LDW1', 'LDW6-LDW7',
--         'LDW7-LDW2', 'LDW7-LDW8', 'LDW8-LDW1', 'LDW8-LDW3'
--     )
--     login ~ '^[125]470'
--     AND (login LIKE '1%' OR login LIKE '2%' OR login LIKE '5%' OR login LIKE 'd%')
--     AND (login NOT LIKE '____9999%')
--     AND (login NOT LIKE '____A9999%')
--     AND (login NOT LIKE '%demo%');

-- SELECT
--     "row_id",
--     "battery_id",
--     "delivery_execution_id",
--     "delivery_id",
--     "is_deleted",
--     "last_update_date",
--     REGEXP_REPLACE("login", '[^0-9]', '', 'g') AS "login",
--     "test_qti_id",
--     "test_qti_label",
--     "test_qti_title",
--     "raw_data",
--     "login_orig"
--   FROM (
--     SELECT "delivery_results".*, "login" AS "login_orig"
--     FROM "oat"."delivery_results"
--     WHERE (SUBSTR("login", 1, 4) = CONCAT_WS('', '1', '008'))
--     LIMIT 10
--   ) AS "q01";

-- SELECT DISTINCT login FROM oat.delivery_results;

-- TRUNCATE TABLE maple.isot_table;

-- WITH tab as (
--     SELECT
--         login, regexp_substr(login,'\d+') username, test_qti_id, delivery_id, raw_data -> 'sessionEndTime' ->> 0 sessionEndtime
--     FROM oat.delivery_results
-- ) SELECT DISTINCT login, username, delivery_id, sessionEndtime FROM tab 
--     WHERE username ~ '^[125]246' AND test_qti_id ~ '^FLA\-';

-- SELECT
--     s.delivery_execution_id, s.delivery_id, s.last_update_date, s.login,
--     s.test_qti_id, s.test_qti_label, s.test_qti_title, s.raw_data, s.raw_data->'metadata'->>'PISA25 Languages' as language
-- FROM oat.delivery_results as s
-- INNER JOIN (
--     SELECT p.username, p.login FROM maple.maple_student_post_val as p
-- ) as p
-- ON p.login = s.login
-- WHERE p.login IS NOT NULL AND p.username ~ '^[125]858' AND s.test_qti_id ~ '^FLA\-.*';

-- SELECT 
--     r.login,
--     r.test_qti_id,
--     r.itemId,
--     r.unit_id,
--     r.resp_cat,
--     r.responses->r.resp_cat->>'value' as db_resp,
--     r.responses->r.resp_cat->>'correct' as db_correct
-- FROM (
--     SELECT
--         b.login,
--         b.test_qti_id,
--         b.itemId,
--         b.unit_id,
--         jsonb_object_keys(b.responses) as resp_cat,
--         b.responses
--     FROM (
--         SELECT
--             t.login, t.itemId, t.test_qti_id,
--             t.raw_data->'items'->itemId->>'qtiLabel' as unit_id,
--             t.raw_data->'items'->itemId->'responses' as responses
--         FROM (
--             SELECT 
--                 login,
--                 regexp_substr(login,'\d+') as username,
--                 raw_data,
--                 test_qti_id,
--                 jsonb_object_keys(raw_data->'items') as itemId
--             FROM oat.delivery_results
--             WHERE test_qti_id ~ '^FLA\-S.*'
--             -- WHERE login ~ 'HS01$|HS02$|FD01$|FD02$' AND test_qti_title = 'FLAR12'
--         ) AS t
--     ) AS b
--     -- WHERE jsonb_typeof(b.responses) = 'object' AND b.unit_id = 'FLARMMB2001'
--     -- WHERE jsonb_typeof(b.responses) = 'object' AND b.login IN (
--     --     SELECT p.login FROM maple.maple_student_post_val as p WHERE p.username ~ '^[125]196'
--     -- )
--     WHERE Login = '12460083008'
-- ) AS r
-- LIMIT 1000;

-- SELECT *
-- FROM oat.delivery_results
-- WHERE login ~ '^1116'
-- LIMIT 10;

-- SELECT x.login,x.itemId,x.unit_id,
--   x.resp_cat,
--   x.responses->x.resp_cat->>'value' as resp_val
--   FROM (
--     SELECT
--         b.login,
--         b.itemId,
--         b.unit_id,
--         jsonb_object_keys(b.responses) as resp_cat,
--         b.responses
--     FROM (
--         SELECT
--             t.login, t.itemId,
--             t.raw_data->'items'->itemId->>'qtiLabel' as unit_id,
--             t.raw_data->'items'->itemId->'responses' as responses
--         FROM (
--             SELECT 
--                 login,
--                 regexp_substr(login,'\d+') as username,
--                 raw_data,
--                 test_qti_id,
--                 jsonb_object_keys(raw_data->'items') as itemId
--             FROM oat.delivery_results
--             -- WHERE test_qti_id ~ '^FLA\-S.*'
--         ) AS t
--         -- WHERE t.username ~ '^[125]196'
--         WHERE t.login = 'A16040013046'
--     ) AS b
--     WHERE jsonb_typeof(b.responses) = 'object'
--     -- AND b.login IN (
--     --     SELECT p.login FROM maple.maple_student_post_val as p WHERE p.username ~ '^[125]196'
--     -- );
--   ) as x;

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
--     z.login,
--     z.username,
--     z.test_qti_id,
--     z.item_id,
--     NULLIF(
--         string_agg(
--             (regexp_match(z.value, 'audio.*'))[1],
--             ', '
--         ),
--         ''
--     ) AS audio_filename
-- FROM (
--     select
--         r.login,
--         r.username,
--         r.test_qti_id,
--         r.item_id,
--         r.response,
--         r.raw_data->'items'->r.itemId->'responses'->r.response->>'value' AS value
--     FROM (
--         SELECT
--             *,
--             t.raw_data->'items'->itemId->>'qtiLabel' as item_id,
--             jsonb_object_keys(t.raw_data->'items'->itemId->'responses') as response
--         FROM (
--             SELECT 
--                 login,
--                 regexp_substr(login,'\d+') as username,
--                 raw_data,
--                 test_qti_id,
--                 jsonb_object_keys(raw_data->'items') as itemId
--             FROM oat.delivery_results
--             WHERE test_qti_id ~ '^FLA\-S.*'
--         ) AS t
--     ) as r
--     WHERE r.item_id ~ '^FLAS'
-- ) as z
-- GROUP BY
--     z.login,
--     z.username,
--     z.test_qti_id,
--     z.item_id;

-- with tab as (
--     SELECT
--         t.raw_data->'items'->itemId->>'qtiLabel' as item_id,
--         t.itemid as clusterId
--     FROM (
--         SELECT 
--             login,
--             regexp_substr(login,'\d+') as username,
--             raw_data,
--             test_qti_id,
--             jsonb_object_keys(raw_data->'items') as itemId
--         FROM oat.delivery_results
--         WHERE test_qti_id ~ '^FLA\-S.*'
--     ) as t
-- )
-- select distinct * from tab
-- where item_id ~ '^FLAS';

select login, raw_data, test_qti_id
from oat.delivery_results
where login = '10560031066' and test_qti_id ~ '^FLA-S';

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



-- SELECT COUNT(*) FROM (SELECT DISTINCT r.login, r.test_qti_id, r.part FROM (
--     SELECT 
--         b.login,
--         b.test_qti_id,
--         b.itemId,
--         b.unit_id,
--         SUBSTRING(b.unit_id,5,1) as part,
--         SUBSTRING(b.unit_id,6,2) as version
--     FROM (
--         SELECT
--             t.login, t.username,t.itemId, t.test_qti_id,
--             t.raw_data->'items'->itemId->>'qtiLabel' as unit_id,
--             t.raw_data->'items'->itemId->'responses' as responses
--         FROM (
--             SELECT 
--                 login,
--                 regexp_substr(login,'\d+') as username,
--                 raw_data,
--                 test_qti_id,
--                 jsonb_object_keys(raw_data->'items') as itemId
--             FROM oat.delivery_results
--             WHERE test_qti_id ~ '^FLA\-S.*'
--             -- WHERE login ~ 'HS01$|HS02$|FD01$|FD02$' AND test_qti_title = 'FLAR12'
--         ) AS t
--     ) AS b
--     WHERE b.username ~ '^1724' AND b.unit_id ~ '^FLAS'
-- ) as r) as u
-- GROUP BY part;

-- SELECT *
-- FROM (
--     SELECT
--         username, qtiLabel, COUNT(*)
--     FROM (
--         SELECT student_login as login, regexp_substr(student_login,'\d+') as username, item_code as qtiLabel
--         FROM "mv_pisa_single_results"
--         WHERE item_code ~ '^FLA'
--     ) AS t
--     group by username, qtiLabel
-- ) as r
-- WHERE r.count > 1;

-- SELECT * FROM oat.delivery_results WHERE login = '1214A9999158';


-- SELECT t.login, t.test_qti_id as testQtilabel, regexp_substr(t.uri,'(?<=test-assembly\-).*?(?=\-)') as test_form
-- FROM (
--     SELECT login, test_qti_id, raw_data->'ltiParameters'->>'spec_lti_claim_target_link_uri' as uri
--     FROM oat.delivery_results
--     WHERE test_qti_id ~ '^FLA-'
-- ) as t;