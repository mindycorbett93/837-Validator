**Healthcare RCM:** 837 Strategic Validator
**Technical Lead:** Melinda Corbett, CPSO, CPC, CPPM, CPB  
**Target Impact:** Pre-submission Claims Audit. Identifies Potential Issues and Denial Risks

# 837 Strategic Validator

ANSI X12 837 Professional claim validator with multi-layer analysis: schema enforcement (Pydantic), structural EDI validation, CPT/modifier clinical audit, and historical denial-risk intelligence powered by a SQLite denial database.

## Repository Structure

```
837_Validator/
├── 837_Validator.py           # Main entry point — 837 file validation pipeline
└── generators/
    ├── generate_837.py        # EDI 837 test data generator (electronic + paper-to-837)
    ├── test_data_commons.py   # Shared data catalog (practice types, CPT codes, payers)
    └── __init__.py
```

## What It Does

1. **Layer 1 — Schema Enforcement** — Pydantic models validate mandatory billing elements (NPI format, CPT codes, modifiers, ICD-10 codes) against ANSI 5010 standards.
2. **Layer 2 — Structural Validation** — Verifies EDI segment ordering, loop integrity, and required segment presence (ISA/GS/ST/CLM/SV1).
3. **Layer 3 — Clinical Audit** — Flags CPT codes missing required modifiers, checks modifier validity, and identifies coding deficiencies as **HIGH AUDIT RISK**.
4. **Layer 4 — Denial Intelligence** — Queries `denials_engine.db` for historical denial patterns by CPT code and payer, flagging matches as **HIGH DENIAL RISK** with CARC/RARC code predictions.

## Output CSV Columns

| Column | Description |
|--------|-------------|
| File | Source 837 filename |
| Claim_PCN | Patient Control Number |
| Claim_Valid | PASS or FAIL |
| Severity | ERROR / HIGH AUDIT RISK / HIGH DENIAL RISK / PASS |
| Category | Finding category (e.g., CPT / Modifier, Denial Pattern) |
| Raw_Code | CPT code (digits only) |
| Risk_Level | High / Medium / Low / None |
| Recovery_Potential | Dollar estimate or N/A |
| Modifier_Required | Yes / No / N/A |
| CARC_RARC_Potential | Predicted CARC + RARC codes |
| Explanation | Human-readable finding description |

## Prerequisites

- Python 3.12+
- pydantic v2 (`pip install pydantic`)

## Cross-Dependencies

- Requires `denials_engine.db` from the **Recoverable_Revenue** group for Layer 4 denial-intelligence lookups. Place the database file in the same directory or pass `--db-path`.

## Usage

```bash
# Generate test 837 data (if needed)
python generators/generate_837.py

# Run validation with defaults
python 837_Validator.py

# Target specific directories
python 837_Validator.py --dirs test_data/837/electronic test_data/837/paper_to_837

# Specify denial database path
python 837_Validator.py --db-path ../Recoverable_Revenue/denials_engine.db
```

## Output

Results are written to `Results/837_Validator/`:
- `837_validation_results_<timestamp>.json` — Full structured findings
- `837_validation_results_<timestamp>.csv` — Categorized findings with severity and risk
- `837_validator.log` — Processing log

## 🎓 About the Author
Melinda Corbett is an Executive Transformation Leader with 12+ years of experience in healthcare operations and AI-driven optimization.She specializes in translating complex aggregate platform data into board-level narratives.
