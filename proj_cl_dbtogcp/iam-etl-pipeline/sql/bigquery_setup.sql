-- ============================================================
-- BigQuery: Create IAM dataset and tables (run once at setup)
-- ============================================================

-- 1. Dataset is created by the DAG automatically,
--    but you can also run this manually:
CREATE SCHEMA IF NOT EXISTS `my-gcp-project.iam_data`
OPTIONS (
  description = "IAM data ingested from PostgreSQL and Oracle",
  location    = "US"
);

-- 2. Audit log table for ETL metadata
CREATE TABLE IF NOT EXISTS `my-gcp-project.iam_data.etl_audit_log` (
    check_name  STRING,
    table_name  STRING,
    metric      STRING,
    value       INT64,
    run_at      TIMESTAMP
);

-- 3. Useful analysis views -----------------------------------------

-- Active users with their roles (denormalised for BI tools)
CREATE OR REPLACE VIEW `my-gcp-project.iam_data.v_active_user_roles` AS
SELECT
    u.user_id,
    u.username,
    u.email,
    u.is_active,
    u.mfa_enabled,
    r.role_id,
    r.role_name,
    r.role_type,
    ur.valid_from,
    ur.valid_to,
    ur.assigned_by
FROM `my-gcp-project.iam_data.users` u
JOIN `my-gcp-project.iam_data.user_roles` ur USING (user_id)
JOIN `my-gcp-project.iam_data.roles`      r  USING (role_id)
WHERE u.is_active = TRUE
  AND ur.is_active = TRUE
  AND (ur.valid_to IS NULL OR ur.valid_to > CURRENT_TIMESTAMP());

-- Permission matrix: which user has which permission via role
CREATE OR REPLACE VIEW `my-gcp-project.iam_data.v_user_permission_matrix` AS
SELECT DISTINCT
    u.user_id,
    u.username,
    p.permission_name,
    p.resource_type,
    p.action,
    r.role_name AS granted_via_role
FROM `my-gcp-project.iam_data.users` u
JOIN `my-gcp-project.iam_data.user_roles`       ur  USING (user_id)
JOIN `my-gcp-project.iam_data.role_permissions` rp  USING (role_id)
JOIN `my-gcp-project.iam_data.permissions`      p   USING (permission_id)
JOIN `my-gcp-project.iam_data.roles`            r   USING (role_id)
WHERE u.is_active = TRUE
  AND ur.is_active = TRUE
  AND rp.is_active = TRUE;

-- Recent audit events (last 30 days)
CREATE OR REPLACE VIEW `my-gcp-project.iam_data.v_recent_audit_logs` AS
SELECT
    l.log_id,
    u.username,
    l.action,
    l.resource_type,
    l.resource_id,
    l.ip_address,
    l.status,
    l.created_at
FROM `my-gcp-project.iam_data.audit_logs` l
LEFT JOIN `my-gcp-project.iam_data.users` u USING (user_id)
WHERE l.created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
ORDER BY l.created_at DESC;

-- ETL load summary (for monitoring dashboard)
CREATE OR REPLACE VIEW `my-gcp-project.iam_data.v_etl_load_summary` AS
SELECT
    DATE(_etl_loaded_at)  AS load_date,
    _etl_source,
    'users'               AS table_name,
    COUNT(*)              AS row_count
FROM `my-gcp-project.iam_data.users`
GROUP BY 1, 2
UNION ALL
SELECT DATE(_etl_loaded_at), _etl_source, 'roles',            COUNT(*) FROM `my-gcp-project.iam_data.roles`            GROUP BY 1,2
UNION ALL
SELECT DATE(_etl_loaded_at), _etl_source, 'user_roles',       COUNT(*) FROM `my-gcp-project.iam_data.user_roles`       GROUP BY 1,2
UNION ALL
SELECT DATE(_etl_loaded_at), _etl_source, 'permissions',      COUNT(*) FROM `my-gcp-project.iam_data.permissions`      GROUP BY 1,2
UNION ALL
SELECT DATE(_etl_loaded_at), _etl_source, 'role_permissions', COUNT(*) FROM `my-gcp-project.iam_data.role_permissions` GROUP BY 1,2
UNION ALL
SELECT DATE(_etl_loaded_at), _etl_source, 'audit_logs',       COUNT(*) FROM `my-gcp-project.iam_data.audit_logs`       GROUP BY 1,2
UNION ALL
SELECT DATE(_etl_loaded_at), _etl_source, 'groups',           COUNT(*) FROM `my-gcp-project.iam_data.groups`           GROUP BY 1,2
UNION ALL
SELECT DATE(_etl_loaded_at), _etl_source, 'user_groups',      COUNT(*) FROM `my-gcp-project.iam_data.user_groups`      GROUP BY 1,2
UNION ALL
SELECT DATE(_etl_loaded_at), _etl_source, 'policies',         COUNT(*) FROM `my-gcp-project.iam_data.policies`         GROUP BY 1,2
ORDER BY 1 DESC, 3;
