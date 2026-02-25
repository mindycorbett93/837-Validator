"""
Generator for EDI 837 Professional (837P) claim test files.

Generates two types of files per practice type:
  1. Electronic 837P files (12 per practice type)
  2. Paper-claim-to-837 converted files (7 per practice type)

Total output: 15 practice types x (12 + 7) = 285 files

All files conform to ANSI X12 5010 format (005010X222A1).
"""

import os
import random
from datetime import timedelta

from generators.test_data_commons import *

# ============================================================
# CONSTANTS
# ============================================================
ELECTRONIC_COUNT = 12
PAPER_COUNT = 7

BASE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "test_data", "837")
ELECTRONIC_DIR = os.path.join(BASE_DIR, "electronic")
PAPER_DIR = os.path.join(BASE_DIR, "paper_to_837")

# Place of service codes
POS_OFFICE = "11"
POS_OUTPATIENT_HOSPITAL = "22"
POS_EMERGENCY = "23"
POS_ASC = "24"
POS_INPATIENT = "21"
POS_LAB = "81"

PRACTICE_POS = {
    "primary_care": [POS_OFFICE, POS_OFFICE, POS_OFFICE, POS_OUTPATIENT_HOSPITAL],
    "internal_medicine": [POS_OFFICE, POS_INPATIENT, POS_OUTPATIENT_HOSPITAL],
    "allergy_immunology": [POS_OFFICE, POS_OFFICE, POS_OUTPATIENT_HOSPITAL],
    "orthopaedics": [POS_OFFICE, POS_ASC, POS_OUTPATIENT_HOSPITAL],
    "cardiology": [POS_OFFICE, POS_INPATIENT, POS_OUTPATIENT_HOSPITAL, POS_ASC],
    "behavioral_health": [POS_OFFICE, POS_OFFICE, POS_OFFICE],
    "radiology": [POS_OUTPATIENT_HOSPITAL, POS_OFFICE, POS_OFFICE],
    "pathology": [POS_LAB, POS_OUTPATIENT_HOSPITAL, POS_OFFICE],
    "gastroenterology": [POS_OFFICE, POS_ASC, POS_OUTPATIENT_HOSPITAL],
    "ob_gyn": [POS_OFFICE, POS_INPATIENT, POS_OUTPATIENT_HOSPITAL],
    "dermatology": [POS_OFFICE, POS_OFFICE, POS_ASC],
    "anesthesiology": [POS_INPATIENT, POS_ASC, POS_OUTPATIENT_HOSPITAL],
    "urgent_care": [POS_OFFICE, POS_EMERGENCY, POS_OFFICE],
    "pediatrics": [POS_OFFICE, POS_OFFICE, POS_OUTPATIENT_HOSPITAL],
    "clinical_laboratory": [POS_LAB, POS_LAB, POS_OFFICE],
}

# Specialties that commonly use onset date (DTP*431)
ONSET_DATE_SPECIALTIES = [
    "orthopaedics", "urgent_care", "cardiology", "gastroenterology",
    "anesthesiology", "behavioral_health",
]

# Modifier pools per specialty
SPECIALTY_MODIFIERS = {
    "primary_care": ["25", "", "", ""],
    "internal_medicine": ["25", "", "", ""],
    "allergy_immunology": ["", "", "", ""],
    "orthopaedics": ["59", "", "RT", "LT"],
    "cardiology": ["26", "TC", "", ""],
    "behavioral_health": ["", "", "", ""],
    "radiology": ["26", "TC", "", ""],
    "pathology": ["26", "TC", "", ""],
    "gastroenterology": ["59", "", "", ""],
    "ob_gyn": ["25", "59", "", ""],
    "dermatology": ["25", "59", "", ""],
    "anesthesiology": ["", "", "", ""],
    "urgent_care": ["25", "", "", ""],
    "pediatrics": ["25", "", "", ""],
    "clinical_laboratory": ["", "", "", ""],
}

# Payer addresses (consistent for each payer)
PAYER_ADDRESSES = {
    "60054": ("PO Box 981106", "El Paso", "TX", "79998"),
    "BCBS1": ("PO Box 105557", "Atlanta", "GA", "30348"),
    "62308": ("PO Box 188061", "Chattanooga", "TN", "37422"),
    "87726": ("PO Box 30555", "Salt Lake City", "UT", "84130"),
    "61101": ("PO Box 14601", "Lexington", "KY", "40512"),
    "CMS": ("PO Box 790040", "St Louis", "MO", "63179"),
    "MDCD": ("PO Box 2078", "Harrisburg", "PA", "17105"),
    "91051": ("PO Box 629028", "El Dorado Hills", "CA", "95762"),
    "ANTM1": ("PO Box 105187", "Atlanta", "GA", "30348"),
    "99726": ("PO Box 7031", "Camden", "SC", "29020"),
}

# Plan type code mapping for SBR segment
PLAN_TYPE_CODES = {
    "PPO": "12",
    "HMO": "HM",
    "Medicare": "MB",
    "Medicaid": "MC",
    "Government": "14",
}


def _get_payer_address(payer_id):
    """Return payer address tuple (street, city, state, zip)."""
    return PAYER_ADDRESSES.get(payer_id, ("PO Box 100000", "Dallas", "TX", "75201"))


def _pick_pos(practice_type, seed):
    """Pick a place-of-service code for this practice and seed."""
    random.seed(seed)
    options = PRACTICE_POS.get(practice_type, [POS_OFFICE])
    return random.choice(options)


def _pick_modifier(practice_type, seed):
    """Pick a modifier (may be empty) appropriate for the specialty."""
    random.seed(seed)
    options = SPECIALTY_MODIFIERS.get(practice_type, ["", "", ""])
    return random.choice(options)


def _build_service_lines(practice_type, practice_idx, file_idx, service_date_str,
                         num_lines, seed, icd_codes):
    """Build service line segments (Loop 2400) and return (segments, total_charge).

    Args:
        practice_type: The specialty key.
        practice_idx: Index of the practice type.
        file_idx: Index of the file within the practice type.
        service_date_str: YYYYMMDD formatted service date.
        num_lines: Number of service lines to generate.
        seed: Random seed for reproducibility.
        icd_codes: List of ICD-10 code tuples for diagnosis pointers.

    Returns:
        Tuple of (list of segment strings, total charge amount).
    """
    random.seed(seed)
    cpt_pool = SPECIALTY_CPT_CODES.get(practice_type, SPECIALTY_CPT_CODES["primary_care"])
    chosen_cpts = random.sample(cpt_pool, min(num_lines, len(cpt_pool)))

    segments = []
    total_charge = 0.0

    for i, (cpt, _desc, base_charge) in enumerate(chosen_cpts, start=1):
        line_num = i
        # Vary charge slightly
        charge = round(base_charge * random.uniform(0.90, 1.10), 2)
        total_charge += charge
        units = 1
        # Some codes have multiple units
        if cpt in ("95004", "95024", "95044", "17003", "90461"):
            units = random.randint(2, 10)
            total_charge += charge * (units - 1)

        modifier = _pick_modifier(practice_type, seed + i * 7)
        if modifier:
            cpt_segment = f"HC:{cpt}:{modifier}"
        else:
            cpt_segment = f"HC:{cpt}"

        # Diagnosis pointer - reference the ICD-10 codes by position (1-based)
        max_ptr = min(len(icd_codes), 4)
        if max_ptr >= 2:
            diag_pointer = f"1:{random.randint(1, max_ptr)}"
        else:
            diag_pointer = "1"

        line_ref = generate_claim_id(practice_idx, file_idx, line_num)

        segments.append(f"LX*{line_num}~")
        segments.append(f"SV1*{cpt_segment}*{charge:.2f}*UN*{units}***{diag_pointer}~")
        segments.append(f"DTP*472*D8*{service_date_str}~")
        segments.append(f"REF*6R*{line_ref}~")

    total_charge = round(total_charge, 2)
    return segments, total_charge


def generate_837p(practice_type, practice_idx, file_idx, seed, is_paper=False):
    """Generate a single 837P EDI file content.

    Args:
        practice_type: Specialty key (e.g. 'primary_care').
        practice_idx: Numeric index of the practice type.
        file_idx: File index within this practice type and category.
        seed: Base random seed for deterministic generation.
        is_paper: If True, generate paper-claim-to-837 variant.

    Returns:
        String containing the complete 837P EDI content.
    """
    random.seed(seed)

    # --- Gather entities ---
    patient = get_random_patient(seed)
    provider = get_random_provider(practice_type, seed + 100)
    payer = get_random_payer(seed + 200)

    # Dates
    service_date = get_random_date(start_days_ago=180, end_days_ago=5, seed=seed + 300)
    service_date_str = format_date_edi(service_date)
    submission_date = service_date + timedelta(days=random.randint(1, 14))
    submission_date_str = format_date_edi(submission_date)
    submission_time = f"{random.randint(0,23):02d}{random.randint(0,59):02d}"

    # Control numbers
    isa_control = f"{(practice_idx * 1000 + file_idx):09d}"
    gs_control = f"{(practice_idx * 1000 + file_idx):09d}"
    st_control = f"{(practice_idx * 100 + file_idx):04d}"

    # ICD-10 codes
    icd_pool = SPECIALTY_ICD10_CODES.get(practice_type, SPECIALTY_ICD10_CODES["primary_care"])
    random.seed(seed + 400)
    num_diag = random.randint(2, 4) if not is_paper else random.randint(1, 3)
    icd_codes = random.sample(icd_pool, min(num_diag, len(icd_pool)))

    # Service lines
    random.seed(seed + 500)
    if is_paper:
        num_lines = random.randint(1, 3)
    else:
        num_lines = random.randint(2, 4)

    line_segments, total_charge = _build_service_lines(
        practice_type, practice_idx, file_idx,
        service_date_str, num_lines, seed + 600, icd_codes
    )

    # Place of service
    if is_paper:
        # Paper claims use office (11) more often
        random.seed(seed + 700)
        pos = POS_OFFICE if random.random() < 0.75 else _pick_pos(practice_type, seed + 700)
    else:
        pos = _pick_pos(practice_type, seed + 700)

    # Payer address
    payer_addr = _get_payer_address(payer["payer_id"])
    plan_type_code = PLAN_TYPE_CODES.get(payer["plan_type"], "12")

    # Provider address
    random.seed(seed + 800)
    prov_street_num = random.randint(100, 9999)
    prov_street = random.choice(STREETS)
    prov_city, prov_state, prov_zip = random.choice(CITIES_STATES_ZIPS)

    # Tax ID
    random.seed(seed + 900)
    tax_id = f"{random.randint(10, 99)}{random.randint(1000000, 9999999)}"

    # Group number
    random.seed(seed + 950)
    group_number = f"GRP{random.randint(100000, 999999)}"

    # Claim reference
    claim_id = generate_claim_id(practice_idx, file_idx)
    claim_ref = f"REF{practice_idx:02d}{file_idx:03d}{random.randint(1000,9999)}"

    # Sender / Receiver IDs (padded to 15 chars)
    sender_id = f"SENDER{practice_idx:03d}".ljust(15)
    receiver_id = f"RECV{payer['payer_id']}".ljust(15)

    # --- Build segments ---
    segments = []

    # === ISA ===
    segments.append(
        f"ISA*00*          *00*          *ZZ*{sender_id}*ZZ*{receiver_id}"
        f"*{submission_date_str[2:]}*{submission_time}*^*00501*{isa_control}*0*T*:~"
    )

    # === GS ===
    segments.append(
        f"GS*HC*SENDER{practice_idx:03d}*RECV{payer['payer_id']}"
        f"*{submission_date_str}*{submission_time}*{gs_control}*X*005010X222A1~"
    )

    # === ST ===
    segments.append(f"ST*837*{st_control}*005010X222A1~")

    # === BHT ===
    segments.append(
        f"BHT*0019*00*{claim_ref}*{submission_date_str}*{submission_time}*CH~"
    )

    # === Loop 1000A - Submitter ===
    segments.append(f"NM1*41*2*{PRACTICE_DISPLAY_NAMES.get(practice_type, practice_type)}*****46*{sender_id.strip()}~")
    segments.append(f"PER*IC*BILLING DEPT*TE*5551234567~")

    # === Loop 1000B - Receiver ===
    segments.append(f"NM1*40*2*{payer['name']}*****46*{payer['payer_id']}~")

    # === Loop 2000A - Billing Provider ===
    segments.append(f"HL*1**20*1~")
    segments.append(f"PRV*BI*PXC*{provider['taxonomy']}~")

    # === Loop 2010AA - Billing Provider Name ===
    segments.append(
        f"NM1*85*1*{provider['last']}*{provider['first']}****XX*{provider['npi']}~"
    )
    segments.append(f"N3*{prov_street_num} {prov_street}~")
    segments.append(f"N4*{prov_city}*{prov_state}*{prov_zip}~")
    segments.append(f"REF*EI*{tax_id}~")

    # === Loop 2000B - Subscriber ===
    segments.append(f"HL*2*1*22*0~")
    segments.append(f"SBR*P*18*{group_number}****{plan_type_code}~")

    # === Loop 2010BA - Subscriber Name ===
    segments.append(
        f"NM1*IL*1*{patient['last']}*{patient['first']}****MI*{patient['member_id']}~"
    )
    segments.append(f"N3*{patient['street']}~")
    segments.append(f"N4*{patient['city']}*{patient['state']}*{patient['zip']}~")
    segments.append(f"DMG*D8*{patient['dob']}*{patient['gender']}~")

    # === Loop 2010BB - Payer ===
    segments.append(
        f"NM1*PR*2*{payer['name']}*****PI*{payer['payer_id']}~"
    )
    segments.append(f"N3*{payer_addr[0]}~")
    segments.append(f"N4*{payer_addr[1]}*{payer_addr[2]}*{payer_addr[3]}~")

    # === Loop 2300 - Claim Information ===
    # Paper claims use 'I' for original reference indicator; electronic use 'Y'
    if is_paper:
        clm_flag = "I"
    else:
        clm_flag = "Y"
    segments.append(
        f"CLM*{claim_id}*{total_charge:.2f}***{pos}:B:1*Y*A*Y*{clm_flag}~"
    )

    # Onset date for relevant specialties
    if practice_type in ONSET_DATE_SPECIALTIES:
        onset_date = service_date - timedelta(days=random.randint(1, 30))
        segments.append(f"DTP*431*D8*{format_date_edi(onset_date)}~")

    # Service date
    segments.append(f"DTP*472*D8*{service_date_str}~")

    # Claim reference
    segments.append(f"REF*D9*{claim_ref}~")

    # Paper-specific segments
    if is_paper:
        random.seed(seed + 1100)
        orig_claim_number = f"ORIG{random.randint(100000000, 999999999)}"
        segments.append(f"REF*F8*{orig_claim_number}~")
        segments.append(f"NTE*ADD*CONVERTED FROM PAPER CLAIM~")

    # Diagnosis codes (HI segments)
    # Primary diagnosis
    segments.append(f"HI*ABK:{icd_codes[0][0]}~")
    # Secondary/additional diagnoses
    if len(icd_codes) > 1:
        for icd_code, _icd_desc in icd_codes[1:]:
            segments.append(f"HI*ABF:{icd_code}~")

    # === Loop 2400 - Service Lines ===
    segments.extend(line_segments)

    # === Closing ===
    # SE segment count includes ST through SE
    segment_count = len(segments) - 2 + 1  # minus ISA and GS, plus SE itself
    segments.append(f"SE*{segment_count}*{st_control}~")
    segments.append(f"GE*1*{gs_control}~")
    segments.append(f"IEA*1*{isa_control}~")

    return "\n".join(segments)


def main():
    """Generate all 837P test files for all practice types."""
    total_electronic = 0
    total_paper = 0

    for practice_idx, practice_type in enumerate(PRACTICE_TYPES):
        # --- Electronic 837P files ---
        electronic_dir = os.path.join(ELECTRONIC_DIR, practice_type)
        os.makedirs(electronic_dir, exist_ok=True)

        for file_idx in range(1, ELECTRONIC_COUNT + 1):
            seed = practice_idx * 10000 + file_idx * 100
            content = generate_837p(
                practice_type=practice_type,
                practice_idx=practice_idx,
                file_idx=file_idx,
                seed=seed,
                is_paper=False,
            )
            filename = f"837p_{practice_type}_{file_idx:03d}.edi"
            filepath = os.path.join(electronic_dir, filename)
            with open(filepath, "w") as f:
                f.write(content)
            total_electronic += 1

        # --- Paper-to-837 converted files ---
        paper_dir = os.path.join(PAPER_DIR, practice_type)
        os.makedirs(paper_dir, exist_ok=True)

        for file_idx in range(1, PAPER_COUNT + 1):
            seed = practice_idx * 10000 + file_idx * 100 + 50000
            content = generate_837p(
                practice_type=practice_type,
                practice_idx=practice_idx,
                file_idx=file_idx,
                seed=seed,
                is_paper=True,
            )
            filename = f"837p_paper_{practice_type}_{file_idx:03d}.edi"
            filepath = os.path.join(paper_dir, filename)
            with open(filepath, "w") as f:
                f.write(content)
            total_paper += 1

    print(f"Generated {total_electronic} electronic 837P files")
    print(f"Generated {total_paper} paper-to-837 converted files")
    print(f"Total: {total_electronic + total_paper} files")


if __name__ == "__main__":
    main()
