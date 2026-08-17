BEGIN;

CREATE TABLE IF NOT EXISTS rfc_quota_reservations (
    reservation_key TEXT PRIMARY KEY,
    family TEXT NOT NULL,
    owner_instance TEXT NOT NULL,
    instance_name TEXT NOT NULL,
    group_jid TEXT NOT NULL,
    shared_key TEXT NOT NULL DEFAULT '',
    count INTEGER NOT NULL DEFAULT 1 CHECK (count > 0),
    wallet_clon_reserved BOOLEAN NOT NULL DEFAULT FALSE,
    status TEXT NOT NULL DEFAULT 'RESERVED'
        CHECK (status IN ('RESERVED', 'COMMITTED', 'RELEASED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_rfc_quota_reservations_active
ON rfc_quota_reservations (
    status,
    family,
    instance_name,
    group_jid,
    shared_key
);

COMMIT;
