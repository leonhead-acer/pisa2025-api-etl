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
--     login, test_qti_id, raw_data
-- FROM oat.delivery_results
-- WHERE test_qti_id ~ '^FLA\-.*'
-- AND login ~ '(?<=^.{1})056'
-- AND login ~ '^[125].*'
-- AND login ~ '^((?!9999).)*$'
-- AND login ~ '^((?!demo).)*$';

-- SELECT
--     login, test_qti_id, raw_data
-- FROM oat.delivery_results
-- WHERE test_qti_id ~ '^FLA\-.*'
-- AND login ~ '(?<=^.{1})056'
-- AND login ~ '^[125].*'
-- AND login ~ '^((?!9999).)*$'
-- AND login ~ '^((?!demo).)*$'
-- LIMIT 10;

WITH tab as (
    SELECT
        row_id, regexp_substr(login,'\d+') login_new, test_qti_id
    FROM oat.delivery_results
) SELECT row_id FROM tab 
    WHERE test_qti_id ~ '^FLA\-.*'
    AND length(login_new) = 11
    AND login_new ~ '(?<=^.{1})056'
    AND login_new ~ '^[125].*'
    AND login_new ~ '^((?!9999).)*$'
    LIMIT 10;

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