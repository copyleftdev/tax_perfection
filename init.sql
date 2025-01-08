-- ===========================================================
-- 1) SCHEMA SETUP
-- ===========================================================
CREATE SCHEMA IF NOT EXISTS ladm;
CREATE SCHEMA IF NOT EXISTS audit;

SET search_path TO ladm, public;

CREATE EXTENSION IF NOT EXISTS postgis;  -- For spatial data


-- ===========================================================
-- 2) TAX RATE AREA (TRA)
-- ===========================================================
CREATE TABLE ladm.TaxRateArea (
    tra_id      BIGSERIAL PRIMARY KEY,
    tra_code    VARCHAR(20) NOT NULL UNIQUE,
    description VARCHAR(255),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_tra_code ON ladm.TaxRateArea(tra_code);

-- ===========================================================
-- 3) PARTIES (LA_Party) - LADM concept
-- ===========================================================
CREATE TABLE ladm.LA_Party (
    party_id          BIGSERIAL PRIMARY KEY,
    party_name        VARCHAR(255) NOT NULL,
    party_type        VARCHAR(50)  NOT NULL,  -- e.g. 'Individual', 'Company'
    identifier        VARCHAR(100),           -- e.g. SSN, Tax ID
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_la_party_name ON ladm.LA_Party(party_name);
CREATE INDEX idx_la_party_identifier ON ladm.LA_Party(identifier);

-- ===========================================================
-- 4) SPATIAL UNITS (LA_SpatialUnit) - LADM concept
-- ===========================================================
CREATE TABLE ladm.LA_SpatialUnit (
    spatial_unit_id   BIGSERIAL PRIMARY KEY,
    geometry          geometry(GEOMETRY, 4326) NOT NULL,
    address           VARCHAR(255),
    cadastral_ref     VARCHAR(100),
    area_sq_m         NUMERIC(12,2),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_la_spatialunit_geom 
    ON ladm.LA_SpatialUnit 
    USING GIST (geometry);

-- ===========================================================
-- 5) BASIC ADMINISTRATIVE UNIT (LA_BAUnit) - LADM concept
-- ===========================================================
CREATE TABLE ladm.LA_BAUnit (
    ba_unit_id             BIGSERIAL PRIMARY KEY,
    unit_name              VARCHAR(255) NOT NULL,
    description            TEXT,
    spatial_unit_id        BIGINT NOT NULL,
    assessor_parcel_number VARCHAR(50) NOT NULL,  -- APN
    tra_id                 BIGINT,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_spatial_unit
        FOREIGN KEY (spatial_unit_id)
        REFERENCES ladm.LA_SpatialUnit(spatial_unit_id),
    CONSTRAINT fk_tra
        FOREIGN KEY (tra_id)
        REFERENCES ladm.TaxRateArea(tra_id)
);

CREATE INDEX idx_baunit_apn 
    ON ladm.LA_BAUnit(assessor_parcel_number);

-- ===========================================================
-- 6) RIGHTS, RESTRICTIONS, RESPONSIBILITIES (LA_RRR) - LADM
-- ===========================================================
CREATE TABLE ladm.LA_RRR (
    rrr_id      BIGSERIAL PRIMARY KEY,
    rrr_type    VARCHAR(100) NOT NULL,  -- e.g. 'Ownership', 'Lease', 'Mortgage'
    ba_unit_id  BIGINT NOT NULL,
    party_id    BIGINT NOT NULL,
    start_date  DATE,
    end_date    DATE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_ba_unit
        FOREIGN KEY (ba_unit_id)
        REFERENCES ladm.LA_BAUnit(ba_unit_id),
    CONSTRAINT fk_party
        FOREIGN KEY (party_id)
        REFERENCES ladm.LA_Party(party_id)
);

CREATE INDEX idx_la_rrr_ba_party 
    ON ladm.LA_RRR(ba_unit_id, party_id);

-- ===========================================================
-- 7) TAX RATE (TaxRate)
-- ===========================================================
CREATE TABLE ladm.TaxRate (
    tax_rate_id     BIGSERIAL PRIMARY KEY,
    rate_name       VARCHAR(100) NOT NULL,   -- e.g. 'Base Prop 13 Rate'
    rate_value      NUMERIC(8,4) NOT NULL,   -- e.g. 0.0100 = 1.00%
    effective_date  DATE NOT NULL,
    expiration_date DATE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_tax_rate_name 
    ON ladm.TaxRate(rate_name);

-- ===========================================================
-- 8) TAX ASSESSMENT (TaxAssessment)
--    Includes Prop 13 info
-- ===========================================================
CREATE TABLE ladm.TaxAssessment (
    assessment_id           BIGSERIAL PRIMARY KEY,
    ba_unit_id              BIGINT NOT NULL,
    assessment_year         INT NOT NULL,
    base_year               INT,
    base_year_value         NUMERIC(14,2),
    prop_13_factor          NUMERIC(5,4) DEFAULT 1.00,
    current_assessed_value  NUMERIC(14,2) NOT NULL,
    roll_type               VARCHAR(20) NOT NULL,  -- 'Secured' or 'Unsecured'
    tax_rate_id             BIGINT NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_ba_unit_assessment
        FOREIGN KEY (ba_unit_id)
        REFERENCES ladm.LA_BAUnit(ba_unit_id),
    CONSTRAINT fk_tax_rate
        FOREIGN KEY (tax_rate_id)
        REFERENCES ladm.TaxRate(tax_rate_id)
);

ALTER TABLE ladm.TaxAssessment 
  ADD CONSTRAINT check_current_value_positive
  CHECK (current_assessed_value >= 0);

CREATE INDEX idx_tax_assessment_ba_year 
    ON ladm.TaxAssessment(ba_unit_id, assessment_year);

-- ===========================================================
-- 9) SUPPLEMENTAL ASSESSMENT (SupplementalAssessment)
-- ===========================================================
CREATE TABLE ladm.SupplementalAssessment (
    supplemental_id   BIGSERIAL PRIMARY KEY,
    ba_unit_id        BIGINT NOT NULL,
    event_date        DATE NOT NULL,
    reason_code       VARCHAR(50),  -- e.g. 'ChangeOfOwnership'
    old_value         NUMERIC(14,2),
    new_value         NUMERIC(14,2),
    difference_value  NUMERIC(14,2),
    tax_rate_id       BIGINT NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_ba_unit_supplemental
        FOREIGN KEY (ba_unit_id)
        REFERENCES ladm.LA_BAUnit(ba_unit_id),
    CONSTRAINT fk_tax_rate_supplemental
        FOREIGN KEY (tax_rate_id)
        REFERENCES ladm.TaxRate(tax_rate_id)
);

CREATE INDEX idx_supplemental_ba_event 
    ON ladm.SupplementalAssessment(ba_unit_id, event_date);

-- ===========================================================
-- 10) TAX BILL (TaxBill)
--     Partitioned by bill_date, now with a default partition
-- ===========================================================
CREATE TABLE ladm.TaxBill (
    bill_date       DATE NOT NULL,    -- partition key
    bill_uid        BIGSERIAL NOT NULL,
    ba_unit_id      BIGINT NOT NULL,
    party_id        BIGINT NOT NULL,
    due_date        DATE NOT NULL,
    amount_due      NUMERIC(14,2) NOT NULL,
    is_paid         BOOLEAN DEFAULT FALSE,
    bill_type       VARCHAR(20) NOT NULL,  -- 'Annual', 'Supplemental'
    supplemental_id BIGINT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Composite PK including partition column
    CONSTRAINT pk_taxbill PRIMARY KEY (bill_date, bill_uid),

    CONSTRAINT fk_ba_unit_bill
        FOREIGN KEY (ba_unit_id)
        REFERENCES ladm.LA_BAUnit(ba_unit_id),
    CONSTRAINT fk_party_bill
        FOREIGN KEY (party_id)
        REFERENCES ladm.LA_Party(party_id),
    CONSTRAINT fk_supplemental
        FOREIGN KEY (supplemental_id)
        REFERENCES ladm.SupplementalAssessment(supplemental_id)
)
PARTITION BY RANGE (bill_date);

-- Example monthly partitions for Jan/Feb 2025:
CREATE TABLE ladm.TaxBill_2025_01 PARTITION OF ladm.TaxBill
FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');

CREATE TABLE ladm.TaxBill_2025_02 PARTITION OF ladm.TaxBill
FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');

--  ADD DEFAULT PARTITION for all other dates
CREATE TABLE ladm.TaxBill_default PARTITION OF ladm.TaxBill
DEFAULT;

-- Optional partial indexes on partitions
CREATE INDEX idx_tax_bill_unpaid_2025_01 
    ON ladm.TaxBill_2025_01(ba_unit_id, party_id)
    WHERE is_paid = FALSE;

CREATE INDEX idx_tax_bill_unpaid_2025_02
    ON ladm.TaxBill_2025_02(ba_unit_id, party_id)
    WHERE is_paid = FALSE;

-- ===========================================================
-- 11) TAX PAYMENT (TaxPayment)
--     Partitioned by payment_date, now with a default partition
-- ===========================================================
CREATE TABLE ladm.TaxPayment (
    payment_date DATE NOT NULL,  -- partition key
    payment_uid  BIGSERIAL NOT NULL,
    
    -- We store BOTH columns to reference TaxBill’s PK
    bill_date DATE NOT NULL,
    bill_uid  BIGINT NOT NULL,

    amount_paid NUMERIC(14,2) NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_taxpayment PRIMARY KEY (payment_date, payment_uid),
    CONSTRAINT fk_tax_bill
        FOREIGN KEY (bill_date, bill_uid)
        REFERENCES ladm.TaxBill(bill_date, bill_uid)
)
PARTITION BY RANGE (payment_date);

-- Example monthly partitions for Jan/Feb 2025:
CREATE TABLE ladm.TaxPayment_2025_01 PARTITION OF ladm.TaxPayment
FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');

CREATE TABLE ladm.TaxPayment_2025_02 PARTITION OF ladm.TaxPayment
FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');

-- ADD DEFAULT PARTITION for all other dates
CREATE TABLE ladm.TaxPayment_default PARTITION OF ladm.TaxPayment
DEFAULT;

CREATE INDEX idx_tax_payment_bill_2025_01 ON ladm.TaxPayment_2025_01(bill_date, bill_uid);
CREATE INDEX idx_tax_payment_bill_2025_02 ON ladm.TaxPayment_2025_02(bill_date, bill_uid);

-- ===========================================================
-- 12) EXEMPTIONS / EXCLUSIONS
-- ===========================================================
CREATE TABLE ladm.Exemption (
    exemption_id      BIGSERIAL PRIMARY KEY,
    ba_unit_id        BIGINT NOT NULL,
    party_id          BIGINT,
    exemption_type    VARCHAR(50) NOT NULL,
    r_t_code_section  VARCHAR(50),
    amount_reduction  NUMERIC(14,2),
    start_date        DATE,
    end_date          DATE,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_ba_unit_exemption
        FOREIGN KEY (ba_unit_id)
        REFERENCES ladm.LA_BAUnit(ba_unit_id),
    CONSTRAINT fk_party_exemption
        FOREIGN KEY (party_id)
        REFERENCES ladm.LA_Party(party_id)
);

CREATE INDEX idx_exemption_type ON ladm.Exemption(exemption_type);

-- ===========================================================
-- 13) AUDIT TRAIL & HISTORY TABLES
-- ===========================================================
CREATE TABLE audit.audit_log (
    audit_id     BIGSERIAL PRIMARY KEY,
    table_name   TEXT NOT NULL,
    operation    TEXT NOT NULL,   -- 'INSERT', 'UPDATE', 'DELETE'
    changed_data JSONB,
    changed_by   TEXT,
    changed_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE OR REPLACE FUNCTION audit.log_changes()
RETURNS TRIGGER AS $$
DECLARE
    row_data JSONB;
BEGIN
    IF (TG_OP = 'DELETE') THEN
        row_data := to_jsonb(OLD);
        INSERT INTO audit.audit_log(table_name, operation, changed_data, changed_by)
        VALUES (TG_TABLE_NAME, TG_OP, row_data, current_user);
        RETURN OLD;
    ELSIF (TG_OP = 'INSERT') THEN
        row_data := to_jsonb(NEW);
        INSERT INTO audit.audit_log(table_name, operation, changed_data, changed_by)
        VALUES (TG_TABLE_NAME, TG_OP, row_data, current_user);
        RETURN NEW;
    ELSE
        -- UPDATE
        row_data := jsonb_build_object('old', to_jsonb(OLD), 'new', to_jsonb(NEW));
        INSERT INTO audit.audit_log(table_name, operation, changed_data, changed_by)
        VALUES (TG_TABLE_NAME, TG_OP, row_data, current_user);
        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Example history table for LA_BAUnit
CREATE TABLE ladm.LA_BAUnit_History (
    history_id     BIGSERIAL PRIMARY KEY,
    ba_unit_id     BIGINT NOT NULL,
    unit_name      VARCHAR(255),
    description    TEXT,
    assessor_parcel_number VARCHAR(50),
    tra_id         BIGINT,
    valid_from     TIMESTAMPTZ NOT NULL,
    valid_to       TIMESTAMPTZ,
    changed_by     TEXT,
    changed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE OR REPLACE FUNCTION ladm.la_baunit_to_history()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'UPDATE' THEN
       INSERT INTO ladm.LA_BAUnit_History(
         ba_unit_id,
         unit_name,
         description,
         assessor_parcel_number,
         tra_id,
         valid_from,
         changed_by
       )
       VALUES(
         OLD.ba_unit_id,
         OLD.unit_name,
         OLD.description,
         OLD.assessor_parcel_number,
         OLD.tra_id,
         CURRENT_TIMESTAMP,
         current_user
       );
    ELSIF TG_OP = 'DELETE' THEN
       INSERT INTO ladm.LA_BAUnit_History(
         ba_unit_id,
         unit_name,
         description,
         assessor_parcel_number,
         tra_id,
         valid_from,
         valid_to,
         changed_by
       )
       VALUES(
         OLD.ba_unit_id,
         OLD.unit_name,
         OLD.description,
         OLD.assessor_parcel_number,
         OLD.tra_id,
         CURRENT_TIMESTAMP,
         CURRENT_TIMESTAMP,
         current_user
       );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Attach triggers for auditing & history
CREATE TRIGGER audit_ba_unit
AFTER INSERT OR UPDATE OR DELETE
ON ladm.LA_BAUnit
FOR EACH ROW
EXECUTE FUNCTION audit.log_changes();

CREATE TRIGGER history_ba_unit
AFTER UPDATE OR DELETE
ON ladm.LA_BAUnit
FOR EACH ROW
EXECUTE FUNCTION ladm.la_baunit_to_history();

-- ===========================================================
-- 14) MATERIALIZED VIEWS (DENORMALIZATION)
-- ===========================================================
CREATE MATERIALIZED VIEW ladm.mv_unpaid_tax_by_apn AS
SELECT 
    b.assessor_parcel_number AS apn,
    SUM(tb.amount_due)       AS total_unpaid
FROM ladm.TaxBill tb
JOIN ladm.LA_BAUnit b 
    ON tb.ba_unit_id = b.ba_unit_id
WHERE tb.is_paid = FALSE
GROUP BY b.assessor_parcel_number;

-- Refresh with: 
-- REFRESH MATERIALIZED VIEW ladm.mv_unpaid_tax_by_apn;

-- ===========================================================
-- 15) CLUSTERING & VACUUM NOTES
-- ===========================================================
-- Example of clustering a partition:
-- CLUSTER ladm.TaxBill_2025_01 USING idx_tax_bill_unpaid_2025_01;
--
-- or custom autovacuum:
-- ALTER TABLE ladm.TaxBill_2025_01
--   SET (autovacuum_vacuum_scale_factor = 0.05);

-- End of schema
