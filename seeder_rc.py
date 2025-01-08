#!/usr/bin/env python3
"""
Database Seeder for LADM-based Property Tax Schema with Riverside-Specific Data
-------------------------------------------------------------------------------

Usage Example:
  python seeder.py --num_parties 50 --num_parcels 100 --num_bills 500

Requirements:
  pip install faker psycopg2-binary

This script:
  1) Connects to your PostgreSQL DB (adjust creds below).
  2) Inserts random data into the LADM-based schema (partitioned tables).
  3) Uses single-row inserts (with RETURNING) to capture IDs.
  4) Incorporates Riverside addresses, local agencies, and a notion of
     "household size" to simulate population.

We assume your database is set up with a schema that includes
DEFAULT partitions (TaxBill_default, TaxPayment_default) so
dates outside any monthly partitions are handled without errors.
"""

import argparse
import random
import string
from datetime import datetime, timedelta

import psycopg2
from faker import Faker

fake = Faker()

# ------------------------------------------------------------
# RIVERSIDE-SPECIFIC DATA
# ------------------------------------------------------------

# Closer bounding box around Riverside city (approx):
MIN_X, MAX_X = -117.50, -117.25
MIN_Y, MAX_Y = 33.85, 34.00

RIVERSIDE_ZIP_CODES = [
    "92501", "92503", "92504", "92505", "92506",
    "92507", "92508", "92509"
]

STREET_SUFFIXES = [
    "St", "Ave", "Blvd", "Rd", "Ln", "Way", "Dr", "Ct"
]

RIVERSIDE_AGENCIES = [
    "Riverside County Assessor's Office",
    "Riverside City Hall",
    "Riverside Water Department",
    "Riverside Unified School District"
]

RIVERSIDE_BUSINESSES = [
    "Mission Inn Hotel & Spa",
    "La Sierra University",
    "Riverside Community Hospital",
    "Galleria at Tyler"
]

def random_point_in_riverside():
    """
    Returns a WKT for a POINT in a bounding box near Riverside city.
    Example: ST_SetSRID(ST_MakePoint(-117.3, 33.9), 4326)
    """
    lng = random.uniform(MIN_X, MAX_X)
    lat = random.uniform(MIN_Y, MAX_Y)
    return f"ST_SetSRID(ST_MakePoint({lng}, {lat}), 4326)"

def generate_riverside_address():
    """
    Returns a realistic Riverside address, e.g.:
    "1234 Magnolia Ave\nRiverside, CA 92506"
    """
    street_num = random.randint(100, 9999)
    # Use part of a Faker street, or just a random word
    street_name = fake.street_name().split()[0]
    suffix = random.choice(STREET_SUFFIXES)
    city = "Riverside"
    state = "CA"
    zip_code = random.choice(RIVERSIDE_ZIP_CODES)
    return f"{street_num} {street_name} {suffix}\n{city}, {state} {zip_code}"

def generate_riverside_party(party_type):
    """
    Returns (party_name, party_type) with local flavor
    for companies / gov agencies or random names for individuals.
    """
    if party_type == "Individual":
        return (fake.name(), "Individual")
    elif party_type == "Company":
        return (random.choice(RIVERSIDE_BUSINESSES), "Company")
    else:  # GovAgency
        return (random.choice(RIVERSIDE_AGENCIES), "GovAgency")


# ------------------------------------------------------------
# DATABASE CONFIG
# ------------------------------------------------------------
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "property_tax_db"
DB_USER = "myuser"
DB_PASS = "mypassword"

def seed_data(num_parties, num_parcels, num_bills):
    """
    Main seeding function. Inserts data for:
      - LA_Party
      - LA_SpatialUnit
      - LA_BAUnit (+ description includes "household size")
      - TaxRateArea, TaxRate
      - TaxAssessment
      - SupplementalAssessment
      - RRR
      - TaxBill (partitioned by bill_date, uses default if out-of-range)
      - TaxPayment (partitioned by payment_date, uses default if out-of-range)
    """
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
    conn.autocommit = False
    cur = conn.cursor()

    try:
        print("=== Seeding Start ===")

        # ----------------------------------------------------
        # 1) PARTIES (LA_Party) with Riverside flavor
        # ----------------------------------------------------
        print("Inserting Parties...")
        party_ids = []
        for _ in range(num_parties):
            chosen_type = random.choice(["Individual", "Company", "GovAgency"])
            party_name, actual_type = generate_riverside_party(chosen_type)
            ident = "".join(random.choices(string.digits, k=9))

            cur.execute("""
                INSERT INTO ladm.LA_Party(party_name, party_type, identifier)
                VALUES (%s, %s, %s)
                RETURNING party_id
            """, (party_name, actual_type, ident))
            new_id = cur.fetchone()[0]
            party_ids.append(new_id)

        # ----------------------------------------------------
        # 2) SPATIAL UNITS (LA_SpatialUnit)
        # ----------------------------------------------------
        print("Inserting SpatialUnits...")
        spatial_ids = []
        for _ in range(num_parcels):
            address = generate_riverside_address()    # custom Riverside address
            geom_wkt = random_point_in_riverside()    # geometry near Riverside
            cadastral_ref = "CAD-" + "".join(random.choices(string.digits, k=6))
            area = random.uniform(200.0, 20000.0)

            cur.execute(f"""
                INSERT INTO ladm.LA_SpatialUnit(geometry, address, cadastral_ref, area_sq_m)
                VALUES ({geom_wkt}, %s, %s, %s)
                RETURNING spatial_unit_id
            """, (address, cadastral_ref, area))
            sid = cur.fetchone()[0]
            spatial_ids.append(sid)

        # ----------------------------------------------------
        # 3) BAUNIT (LA_BAUnit)
        #   We'll add a "household_size" to the "description" for fun
        # ----------------------------------------------------
        print("Inserting BAUnits...")
        baunit_ids = []
        for sid in spatial_ids:
            unit_name = "Parcel " + "".join(random.choices(string.ascii_uppercase, k=4))
            apn = (
                "".join(random.choices(string.digits, k=3))
                + "-"
                + "".join(random.choices(string.digits, k=3))
                + "-"
                + "".join(random.choices(string.digits, k=3))
            )
            # Random "household size" to simulate population
            household_size = random.randint(1, 6)
            desc = f"Household Size: {household_size}"

            cur.execute("""
                INSERT INTO ladm.LA_BAUnit(
                    unit_name, spatial_unit_id, assessor_parcel_number, description
                )
                VALUES (%s, %s, %s, %s)
                RETURNING ba_unit_id
            """, (unit_name, sid, apn, desc))
            buid = cur.fetchone()[0]
            baunit_ids.append(buid)

        # ----------------------------------------------------
        # 4) TAX RATE AREA + TAX RATE
        # ----------------------------------------------------
        print("Inserting TaxRateArea & TaxRate...")

        tra_codes = [
            ("TRA_001", "Riverside City District"),
            ("TRA_002", "Murrieta District"),
            ("TRA_003", "Coachella District")
        ]
        tra_ids_map = {}
        for code, desc in tra_codes:
            cur.execute("""
                INSERT INTO ladm.TaxRateArea(tra_code, description)
                VALUES (%s, %s)
                RETURNING tra_id
            """, (code, desc))
            tra_id_new = cur.fetchone()[0]
            tra_ids_map[code] = tra_id_new

        tax_rates_data = [
            ("Base Prop 13 Rate", 0.0100, "2020-01-01", None),
            ("Local Bond Measure", 0.0015, "2020-01-01", None),
            ("Special School Tax", 0.0008, "2020-01-01", None)
        ]
        tax_rate_ids = []
        for name, val, eff_date, exp_date in tax_rates_data:
            cur.execute("""
                INSERT INTO ladm.TaxRate(rate_name, rate_value, effective_date, expiration_date)
                VALUES (%s, %s, %s, %s)
                RETURNING tax_rate_id
            """, (name, val, eff_date, exp_date))
            tax_rate_ids.append(cur.fetchone()[0])

        # Optionally assign a random TRA to each BAUnit
        for buid in baunit_ids:
            assigned_tra = random.choice(list(tra_ids_map.values()))
            cur.execute("""
                UPDATE ladm.LA_BAUnit
                SET tra_id = %s
                WHERE ba_unit_id = %s
            """, (assigned_tra, buid))

        # ----------------------------------------------------
        # 5) TAX ASSESSMENT (One per BAUnit)
        # ----------------------------------------------------
        print("Inserting TaxAssessments...")
        for buid in baunit_ids:
            assessment_year = random.choice([2023, 2024, 2025])
            base_year = random.choice([2018, 2019, 2020])
            base_val = random.uniform(100000, 800000)
            factor = 1.0 + (random.random() * 0.1)  # up to 10% bump
            current_val = base_val * factor
            roll_type = random.choice(["Secured", "Unsecured"])
            used_rate = random.choice(tax_rate_ids)

            cur.execute("""
                INSERT INTO ladm.TaxAssessment(
                    ba_unit_id, assessment_year, base_year, base_year_value,
                    prop_13_factor, current_assessed_value, roll_type, tax_rate_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (buid, assessment_year, base_year, base_val,
                  factor, current_val, roll_type, used_rate))

        # ----------------------------------------------------
        # 6) SUPPLEMENTAL ASSESSMENT (random subset)
        # ----------------------------------------------------
        print("Inserting SupplementalAssessments...")
        subset_for_supp = random.sample(baunit_ids, k=min(len(baunit_ids)//5, 10))
        for buid in subset_for_supp:
            reason = random.choice(["ChangeOfOwnership", "NewConstruction"])
            old_val = random.uniform(100000, 600000)
            new_val = old_val + random.uniform(10000, 60000)
            diff_val = new_val - old_val
            used_rate = random.choice(tax_rate_ids)
            ev_date = fake.date_between(start_date="-2y", end_date="today")

            cur.execute("""
                INSERT INTO ladm.SupplementalAssessment(
                    ba_unit_id, event_date, reason_code,
                    old_value, new_value, difference_value, tax_rate_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (buid, ev_date, reason, old_val, new_val, diff_val, used_rate))

        # ----------------------------------------------------
        # 7) RRR (Ownership record for each BAUnit)
        # ----------------------------------------------------
        print("Inserting RRR (Ownership)...")
        for buid in baunit_ids:
            party_choice = random.choice(party_ids)
            start_dt = fake.date_between(start_date="-10y", end_date="today")
            cur.execute("""
                INSERT INTO ladm.LA_RRR(rrr_type, ba_unit_id, party_id, start_date)
                VALUES (%s, %s, %s, %s)
            """, ("Ownership", buid, party_choice, start_dt))

        # ----------------------------------------------------
        # 8) TAX BILLS (Partitioned by bill_date, with default partition)
        # ----------------------------------------------------
        print("Inserting TaxBills...")
        inserted_bills = []
        for _ in range(num_bills):
            buid = random.choice(baunit_ids)
            pid = random.choice(party_ids)
            # This date can be anywhere in the last 3 years
            bill_dt = fake.date_between(start_date="-3y", end_date="today")
            due_dt = bill_dt + timedelta(days=30)
            amt_due = random.uniform(500, 5000)
            is_paid = False
            bill_type = random.choice(["Annual", "Supplemental"])
            supplemental_id = None

            cur.execute("""
                INSERT INTO ladm.TaxBill(
                    bill_date, ba_unit_id, party_id,
                    due_date, amount_due, is_paid, bill_type, supplemental_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING bill_date, bill_uid
            """, (bill_dt, buid, pid, due_dt, amt_due, is_paid, bill_type, supplemental_id))
            inserted_bills.append(cur.fetchone())  # (bill_date, bill_uid)

        # ----------------------------------------------------
        # 9) TAX PAYMENTS (Partitioned by payment_date)
        # ----------------------------------------------------
        print("Inserting TaxPayments...")
        half_bills = random.sample(inserted_bills, k=len(inserted_bills)//2)
        for (b_date, b_uid) in half_bills:
            how_many = random.randint(1, 2)
            for _ in range(how_many):
                pay_dt = fake.date_between(start_date=b_date, end_date="today")
                amt_paid = random.uniform(50, 1000)

                cur.execute("""
                    INSERT INTO ladm.TaxPayment(
                        payment_date, bill_date, bill_uid,
                        amount_paid
                    )
                    VALUES (%s, %s, %s, %s)
                """, (pay_dt, b_date, b_uid, amt_paid))

        # Optionally mark some bills as paid
        # (Naive logic: randomly set is_paid, ignoring total payments)
        for (b_date, b_uid) in inserted_bills:
            if random.random() < 0.5:
                cur.execute("""
                    UPDATE ladm.TaxBill
                    SET is_paid = TRUE
                    WHERE bill_date = %s AND bill_uid = %s
                """, (b_date, b_uid))

        conn.commit()
        print("=== Seeding Completed Successfully! ===")

    except Exception as e:
        conn.rollback()
        print("Error during seeding. Rolled back transaction.")
        raise e
    finally:
        cur.close()
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Seed the LADM-based property tax DB with Riverside-specific, realistic data."
    )
    parser.add_argument("--num_parties", type=int, default=50,
                        help="Number of LA_Party records")
    parser.add_argument("--num_parcels", type=int, default=100,
                        help="Number of BAUnits (and SpatialUnits)")
    parser.add_argument("--num_bills", type=int, default=500,
                        help="Number of TaxBills to create")
    args = parser.parse_args()

    seed_data(args.num_parties, args.num_parcels, args.num_bills)


if __name__ == "__main__":
    main()
