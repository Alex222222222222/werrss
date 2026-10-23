-- Imported account settings can exist before credentials are enrolled.
ALTER TABLE weread_accounts
    ALTER COLUMN credentials_ciphertext DROP NOT NULL,
    ALTER COLUMN access_expires_at DROP NOT NULL,
    ADD COLUMN setup_required BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE weread_accounts
    DROP CONSTRAINT IF EXISTS weread_accounts_credentials_ciphertext_check,
    DROP CONSTRAINT IF EXISTS weread_accounts_credentials_ciphertext_check1,
    ADD CONSTRAINT weread_accounts_setup_state_check CHECK (
        (setup_required AND credentials_ciphertext IS NULL AND access_expires_at IS NULL)
        OR
        (NOT setup_required AND credentials_ciphertext IS NOT NULL
            AND octet_length(credentials_ciphertext) >= 1 AND access_expires_at IS NOT NULL)
    );

DROP INDEX IF EXISTS weread_accounts_expiry_idx;
CREATE INDEX weread_accounts_expiry_idx
    ON weread_accounts (access_expires_at ASC, account_id ASC)
    WHERE NOT disabled AND NOT setup_required;
