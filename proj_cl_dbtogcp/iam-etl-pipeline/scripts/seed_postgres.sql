-- =============================================================================
-- Seed script: creates IAM schema & sample data in local dev PostgreSQL
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS iam;

-- Users
CREATE TABLE IF NOT EXISTS iam.users (
    user_id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username               VARCHAR(100) NOT NULL UNIQUE,
    email                  VARCHAR(255),
    display_name           VARCHAR(255),
    is_active              BOOLEAN DEFAULT TRUE,
    is_locked              BOOLEAN DEFAULT FALSE,
    mfa_enabled            BOOLEAN DEFAULT FALSE,
    password_last_changed  TIMESTAMPTZ,
    last_login_at          TIMESTAMPTZ,
    created_at             TIMESTAMPTZ DEFAULT NOW(),
    updated_at             TIMESTAMPTZ DEFAULT NOW()
);

-- Roles
CREATE TABLE IF NOT EXISTS iam.roles (
    role_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    role_name      VARCHAR(100) NOT NULL UNIQUE,
    description    TEXT,
    role_type      VARCHAR(50),
    is_system_role BOOLEAN DEFAULT FALSE,
    parent_role_id UUID REFERENCES iam.roles(role_id),
    created_at     TIMESTAMPTZ DEFAULT NOW(),
    updated_at     TIMESTAMPTZ DEFAULT NOW()
);

-- User Roles
CREATE TABLE IF NOT EXISTS iam.user_roles (
    assignment_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id       UUID REFERENCES iam.users(user_id),
    role_id       UUID REFERENCES iam.roles(role_id),
    assigned_by   UUID,
    valid_from    TIMESTAMPTZ DEFAULT NOW(),
    valid_to      TIMESTAMPTZ,
    is_active     BOOLEAN DEFAULT TRUE,
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    updated_at    TIMESTAMPTZ DEFAULT NOW()
);

-- Permissions
CREATE TABLE IF NOT EXISTS iam.permissions (
    permission_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    permission_name      VARCHAR(200) NOT NULL UNIQUE,
    resource_type        VARCHAR(100),
    action               VARCHAR(100),
    description          TEXT,
    is_system_permission BOOLEAN DEFAULT FALSE,
    created_at           TIMESTAMPTZ DEFAULT NOW(),
    updated_at           TIMESTAMPTZ DEFAULT NOW()
);

-- Role Permissions
CREATE TABLE IF NOT EXISTS iam.role_permissions (
    rp_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    role_id       UUID REFERENCES iam.roles(role_id),
    permission_id UUID REFERENCES iam.permissions(permission_id),
    granted_by    UUID,
    granted_at    TIMESTAMPTZ DEFAULT NOW(),
    is_active     BOOLEAN DEFAULT TRUE,
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    updated_at    TIMESTAMPTZ DEFAULT NOW()
);

-- Audit Logs
CREATE TABLE IF NOT EXISTS iam.audit_logs (
    log_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id       UUID,
    action        VARCHAR(200),
    resource_type VARCHAR(100),
    resource_id   VARCHAR(200),
    ip_address    INET,
    user_agent    TEXT,
    status        VARCHAR(50),
    details       JSONB,
    created_at    TIMESTAMPTZ DEFAULT NOW()
);

-- ── Sample Data ───────────────────────────────────────────────────────────────
INSERT INTO iam.roles (role_name, description, role_type, is_system_role) VALUES
  ('admin',       'Full system access',          'system',   TRUE),
  ('read_only',   'Read-only access',            'system',   TRUE),
  ('iam_manager', 'Manages IAM resources',       'custom',   FALSE),
  ('auditor',     'View audit logs and reports', 'custom',   FALSE)
ON CONFLICT DO NOTHING;

INSERT INTO iam.users (username, email, display_name, is_active, mfa_enabled) VALUES
  ('alice',   'alice@example.com',   'Alice Smith',   TRUE,  TRUE),
  ('bob',     'bob@example.com',     'Bob Jones',     TRUE,  FALSE),
  ('charlie', 'charlie@example.com', 'Charlie Brown', FALSE, FALSE),
  ('diana',   'diana@example.com',   'Diana Prince',  TRUE,  TRUE)
ON CONFLICT DO NOTHING;

INSERT INTO iam.permissions (permission_name, resource_type, action) VALUES
  ('users:read',        'user',   'read'),
  ('users:write',       'user',   'write'),
  ('users:delete',      'user',   'delete'),
  ('roles:read',        'role',   'read'),
  ('roles:write',       'role',   'write'),
  ('audit_logs:read',   'audit',  'read')
ON CONFLICT DO NOTHING;
