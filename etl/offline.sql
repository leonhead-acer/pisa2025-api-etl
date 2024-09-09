SELECT * from (
    select  row_id
    ,battery_id
    ,delivery_execution_id
    ,delivery_id
    ,is_deleted
    ,last_update_date
    ,login
    ,test_qti_id
    ,test_qti_label
    ,test_qti_title
    ,raw_data
    ,raw_data->'ltiParameters'->>'spec_lti_claim_custom_deliverySettings_attemptId' as offline
    from oat.delivery_results
    WHERE test_qti_id ~ '^FLA-'
) as t
where t.offline ~ '^VM_';