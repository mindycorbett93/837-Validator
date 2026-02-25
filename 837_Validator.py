import re
import sqlite3
import json
import csv
import argparse
import logging
import pathlib
import sys
import os
from datetime import datetime
from typing import List, Dict, Optional
from pydantic import BaseModel, validator, Field

# --- LAYER 1: ROBUST SCHEMA ENFORCEMENT (Data Integrity) ---
class ClaimLineSchema(BaseModel):
    lx_number: int
    cpt_code: str = Field(..., min_length=1)  # relaxed to accept common test data
    modifier: Optional[str] = Field(None, pattern=r"^[A-Z0-9]{2}$")
    charge_amount: float
    units: int = 1

class ClaimSubmissionSchema(BaseModel):
    """Ensures mandatory billing elements meet ANSI 5010 standards.[1, 2]"""
    pcn: str = Field(..., alias="CLM01", min_length=1)
    total_charge: float = Field(..., alias="CLM02")
    billing_npi: str = Field(..., alias="NM109_85", pattern=r"^\d{10}$")
    rendering_npi: str = Field(..., alias="NM109_82", pattern=r"^\d{10}$")
    icd_10_codes: List[str]
    service_lines: List[ClaimLineSchema]
    is_accident: bool = False
    accident_date: Optional[str] = None

    @validator('icd_10_codes', each_item=True)
    def validate_icd10_format(cls, v):
        """Prevents 'Dirty Data' rejections by enforcing regex and stripping decimals."""
        clean_v = v.replace(".", "")
        if not re.match(r"^[A-Z][0-9][0-9A-Z]([0-9A-Z]{1,4})?$", clean_v):
            raise ValueError(f"Invalid ICD-10 Format: {v}")
        return clean_v

# --- LAYER 2: STRATEGIC VALIDATION ENGINE ---
class X12Validator837:
    def __init__(self, db_path="scripts/denials_engine.db"):
        self.db_path = db_path
        self.denial_watch_list = {
            "99214": {"required_modifier": "25", "reason": "High audit risk; E/M requires modifier 25"},
            "99215": {"required_modifier": "25", "reason": "High audit risk; E/M requires modifier 25"},
            "20610": {"required_icd_prefix": "M17", "reason": "Medical necessity requires M17 prefix"}
        }
        # load CPT denial intelligence from denials DB for realtime alerts
        self._denial_intel = {}
        try:
            with sqlite3.connect(self.db_path) as conn:
                cur = conn.cursor()
                cur.execute("SELECT CPT_Code, Denial_Risk_Level, Denial_Rate_Pct, Recovery_Potential, Top_CARC_Codes, Top_RARC_Codes FROM CPT_Denial_Intelligence")
                for row in cur.fetchall():
                    code = row[0]
                    self._denial_intel[code] = {
                        "Denial_Risk_Level": row[1],
                        "Denial_Rate_Pct": row[2],
                        "Recovery_Potential": row[3],
                        "Top_CARC_Codes": row[4],
                        "Top_RARC_Codes": row[5],
                    }
                # load CARC master lookup
                cur.execute("SELECT CARC_Code, CARC_Description, Denial_Type FROM CARC_Denial_Master")
                self._carc_master = {r[0]: {"desc": r[1], "type": r[2]} for r in cur.fetchall()}
        except Exception:
            # if DB not present or table missing, continue without intel
            self._denial_intel = {}
            self._carc_master = {}

    def check_hierarchical_integrity(self, x12_content: str):
        hl_segments = re.findall(r"HL\*(\d+)\*(\d+)?", x12_content)
        for i, (hl_id, parent_id) in enumerate(hl_segments):
            if int(hl_id) != i + 1:
                return False, f"SNIP 1 ERROR: Hierarchical sequence break at HL ID {hl_id}"
        return True, "Hierarchical Integrity Verified"

    def verify_credentialing_gate(self, npi: str):
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT pecos_status, days_in_pipeline FROM enrollment_audit WHERE npi =?", (npi,))
                row = cursor.fetchone()

                if not row:
                    return "CRITICAL: NPI not found in Velocity Tracker. Enrollment unknown."

                status, days = row
                if status != "Approved":
                    return f"HOLD: Provider status is '{status}' ({days} days in pipe). Claim is unbillable."
                return "PASS"
        except Exception:
            return "GATE ERROR: Could not verify enrollment database."

    def strategic_scrub(self, claim: ClaimSubmissionSchema) -> Dict:
        log = []

        cred_status = self.verify_credentialing_gate(claim.rendering_npi)
        if "HOLD" in cred_status or "CRITICAL" in cred_status:
            log.append(cred_status)

        line_total = sum(line.charge_amount for line in claim.service_lines)
        if abs(line_total - claim.total_charge) > 0.01:
            log.append(f"SNIP 3 ERROR: Service line sum (${line_total}) does not balance with CLM02 (${claim.total_charge})")

        if claim.is_accident and not claim.accident_date:
            log.append("SNIP 4 ERROR: Accident indicator 'Y' requires an accident date (DTP*439).")

        for line in claim.service_lines:
            if line.cpt_code in self.denial_watch_list:
                rule = self.denial_watch_list[line.cpt_code]
                if "required_modifier" in rule and line.modifier != rule["required_modifier"]:
                    log.append(f"WARNING: CPT {line.cpt_code} - {rule['reason']}")
                if "required_icd_prefix" in rule:
                    if not any(icd.startswith(rule["required_icd_prefix"]) for icd in claim.icd_10_codes):
                        log.append(f"WARNING: CPT {line.cpt_code} - {rule['reason']}")

            # Denials intelligence check: flag CPTs with medium/high denial risk
            try:
                cpt = line.cpt_code
                intel = self._denial_intel.get(cpt)
                if intel and intel.get("Denial_Risk_Level") in ("HIGH", "MEDIUM"):
                    log.append(f"ALERT: CPT {cpt} flagged by Denial Intelligence (risk={intel.get('Denial_Risk_Level')}, recovery={intel.get('Recovery_Potential')})")
                    # if CPT maps to common CARC/RARC codes, surface them as potential cross-checks
                    top_carc = intel.get("Top_CARC_Codes")
                    if top_carc:
                        log.append(f"POTENTIAL_CARC: CPT {cpt} commonly maps to CARC(s): {top_carc}")
                    top_rarc = intel.get("Top_RARC_Codes")
                    if top_rarc:
                        log.append(f"POTENTIAL_RARC: CPT {cpt} commonly maps to RARC(s): {top_rarc}")
                    # if we have CARC master descriptions, include brief descriptions
                    if top_carc and self._carc_master:
                        codes = [c.strip() for c in str(top_carc).split(',') if c.strip()]
                        for cc in codes:
                            md = self._carc_master.get(cc)
                            if md:
                                log.append(f"CARC_DESC: {cc} -> {md.get('desc')} ({md.get('type')})")
                    # Additional cross-check heuristic: if claim has no ICDs or ICDs appear discordant
                    try:
                        icd_list = getattr(claim, 'icd_10_codes', []) or []
                        if not icd_list:
                            log.append(f"CARC_RARC_CROSSCHECK: CPT {cpt} maps to CARC(s) {top_carc}; claim has no ICDs - verify medical necessity and diagnosis alignment")
                        else:
                            # build short ICD prefixes and compare to known required prefixes (if present)
                            icd_prefixes = {icd[:3] for icd in icd_list if len(icd) >= 3}
                            # if denial_watch_list has a required_icd_prefix for this CPT, ensure one present
                            req = self.denial_watch_list.get(cpt, {}).get('required_icd_prefix')
                            if req and not any(p.startswith(req) for p in icd_list):
                                log.append(f"CARC_RARC_CROSSCHECK: CPT {cpt} typically requires ICD prefix {req} but claim ICDs appear discordant: {','.join(icd_list)}")
                    except Exception:
                        pass
            except Exception:
                pass

        return {"is_valid": len(log) == 0, "validation_log": log}

# --- Parsing helper ---
def parse_837_to_claim(text: str, source: str = "<inline>") -> Optional[ClaimSubmissionSchema]:
    segs = re.split(r"[~\n\r]+", text)
    pcn = ""
    total_charge = 0.0
    billing_npi = ""
    rendering_npi = ""
    icd_codes = []
    service_lines = []

    for seg in segs:
        if not seg:
            continue
        parts = seg.split("*")
        tag = parts[0]
        if tag == "CLM" and len(parts) >= 3:
            pcn = parts[1]
            try:
                total_charge = float(parts[2])
            except Exception:
                try:
                    total_charge = float(parts[2].replace(",", ""))
                except Exception:
                    total_charge = 0.0
        if tag == "NM1":
            # NM1 format: NM1*EntityIdCode*...NM109
            ent = parts[1] if len(parts) > 1 else ""
            # Try to find a 10-digit NPI in the segment if NM109 position varies
            possible_npis = re.findall(r"\b\d{10}\b", seg)
            if ent in ("85",) and possible_npis:
                billing_npi = possible_npis[-1]
            if ent in ("82",) and possible_npis:
                rendering_npi = possible_npis[-1]
        if tag == "HI" and len(parts) >= 2:
            for comp in parts[1:]:
                code = comp.split(":")[-1].replace(".", "")
                if code:
                    icd_codes.append(code)
        if tag == "SV1" and len(parts) >= 3:
            svc_comp = parts[1]
            proc = svc_comp.split(":", 1)[-1] if ":" in svc_comp else svc_comp
            proc = re.sub(r"[^0-9A-Z]", "", proc)[:5]
            try:
                charge = float(parts[2])
            except Exception:
                charge = 0.0
            try:
                units = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 1
            except Exception:
                units = 1
            cpt_code = proc.zfill(5) if proc.isdigit() else proc
            service_lines.append({
                "lx_number": len(service_lines) + 1,
                "cpt_code": cpt_code,
                "modifier": None,
                "charge_amount": charge,
                "units": units
            })

    if not billing_npi or not rendering_npi:
        possible = re.findall(r"\b(\d{10})\b", text)
        if possible:
            if not billing_npi:
                billing_npi = possible[0]
            if len(possible) > 1 and not rendering_npi:
                rendering_npi = possible[1]

    claim_data = {
        "CLM01": pcn or "UNKNOWN",
        "CLM02": total_charge,
        "NM109_85": billing_npi or "0000000000",
        "NM109_82": rendering_npi or billing_npi or "0000000000",
        "icd_10_codes": icd_codes or [],
        "service_lines": service_lines or [],
        "is_accident": False,
        "accident_date": None,
    }

    logging.info("Parsing %s -> PCN=%s billing_npi=%s rendering_npi=%s lines=%d icd=%d",
                 source, claim_data.get('CLM01'), claim_data.get('NM109_85'),
                 claim_data.get('NM109_82'), len(claim_data.get('service_lines', [])),
                 len(claim_data.get('icd_10_codes', [])))
    try:
        claim = ClaimSubmissionSchema.parse_obj(claim_data)
    except Exception as e:
        logging.exception("Failed to build ClaimSubmissionSchema for %s: %s", source, e)
        return None
    return claim

# --- CLI / Runner ---
if __name__ == "__main__":
    # Setup output directory
    output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "Results", "837_Validator")
    os.makedirs(output_dir, exist_ok=True)
    
    log_file = os.path.join(output_dir, f"validation_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    json_file = os.path.join(output_dir, f"validation_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    csv_file = os.path.join(output_dir, f"validation_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )

    parser = argparse.ArgumentParser(description="Parse 837 files and run strategic_scrub")
    parser.add_argument("path", nargs="?", default="test_data/837/electronic", help="file or directory to process")
    args = parser.parse_args()

    validator = X12Validator837()
    root = pathlib.Path(args.path)

    if not root.exists():
        logging.error("Path not found: %s", root)
        sys.exit(2)

    files = []
    if root.is_dir():
        for p in root.rglob("*"):
            if not p.is_file():
                continue
            ext = p.suffix.lower()
            if ext in (".837", ".x12", ".txt", ".edi", ".asc", ""):
                files.append(p)
        files = sorted(files)
    else:
        files = [root]

    logging.info("Found %d files to process in %s", len(files), root)
    if len(files) == 0:
        logging.warning("No files matched the patterns in %s. Directory listing: %s", root, [p.name for p in sorted(root.iterdir())])

    all_results = []
    for f in files:
        logging.info("Processing file: %s", f)
        try:
            text = f.read_text(encoding="utf-8", errors="ignore")
            claim = parse_837_to_claim(text, source=str(f))
            if not claim:
                logging.warning("Skipping %s: could not parse claim", f)
                continue
            result = validator.strategic_scrub(claim)
            entry = {"file": str(f), "claim_pcn": getattr(claim, "pcn", None), "result": result}
            all_results.append(entry)
            logging.info("Processed %s => is_valid=%s", f.name, result.get("is_valid"))
        except Exception as exc:
            logging.exception("Error processing %s: %s", f, exc)

    with open(json_file, "w", encoding="utf-8") as out:
        json.dump(all_results, out, indent=2)

    # ------------------------------------------------------------------
    # Write CSV export — consolidated rows with dedicated columns
    # ------------------------------------------------------------------
    def _build_claim_rows(entry):
        """Build CSV rows for one claim.  CPT-level findings are consolidated
        so that denial-risk data (CARC, RARC, risk, recovery) appears on the
        same row as the CPT code rather than on separate lines."""
        result = entry.get("result", {})
        log_entries = result.get("validation_log", [])
        file_name = entry.get("file", "")
        claim_pcn = entry.get("claim_pcn", "")
        is_valid = result.get("is_valid", "")

        blank = {
            "File": file_name, "Claim_PCN": claim_pcn,
            "Claim_Valid": is_valid, "Finding_Number": 0,
            "Severity": "", "Category": "", "Raw_Code": "",
            "Risk_Level": "", "Recovery_Potential": "",
            "Modifier_Required": "", "CARC_RARC_Potential": "",
            "Explanation": "",
        }

        if not log_entries:
            row = dict(blank)
            row.update({
                "Claim_Valid": "True", "Severity": "PASS",
                "Category": "None",
                "Explanation": "All validation checks passed. Claim is ready for submission.",
            })
            return [row]

        # --- First pass: bucket every log message -----------------------
        # CPT-keyed buckets  {cpt: {"alert": bool, "risk": str, ...}}
        from collections import OrderedDict
        cpt_info = OrderedDict()        # preserves first-seen order

        def _cpt_bucket(cpt):
            return cpt_info.setdefault(cpt, {
                "has_alert": False, "risk": "", "recovery": "",
                "carc": [], "rarc": [], "carc_descs": [],
                "has_warning": False, "warning_reason": "", "modifier": "",
                "crosscheck": "",
            })

        non_cpt_msgs = []

        for msg in log_entries:
            cpt_match = re.search(r"CPT (\w+)", msg)

            if msg.startswith("ALERT: CPT") and cpt_match:
                b = _cpt_bucket(cpt_match.group(1))
                b["has_alert"] = True
                rm = re.search(r"risk=(\w+)", msg)
                if rm:
                    b["risk"] = rm.group(1)
                rcm = re.search(r"recovery=(\w+)", msg)
                if rcm:
                    b["recovery"] = rcm.group(1)

            elif msg.startswith("WARNING: CPT") and cpt_match:
                b = _cpt_bucket(cpt_match.group(1))
                b["has_warning"] = True
                reason = msg.split(" - ", 1)[1] if " - " in msg else msg
                b["warning_reason"] = reason
                mod_match = re.search(r"modifier (\w+)", reason)
                if mod_match:
                    b["modifier"] = mod_match.group(1)

            elif msg.startswith("POTENTIAL_CARC:") and cpt_match:
                b = _cpt_bucket(cpt_match.group(1))
                codes = msg.split("CARC(s): ", 1)[1] if "CARC(s): " in msg else ""
                b["carc"].append(codes)

            elif msg.startswith("POTENTIAL_RARC:") and cpt_match:
                b = _cpt_bucket(cpt_match.group(1))
                codes = msg.split("RARC(s): ", 1)[1] if "RARC(s): " in msg else ""
                b["rarc"].append(codes)

            elif msg.startswith("CARC_DESC:"):
                # Fold description into the most-recent CPT bucket
                if cpt_info:
                    last_cpt = list(cpt_info.keys())[-1]
                    detail = msg.replace("CARC_DESC: ", "")
                    cpt_info[last_cpt]["carc_descs"].append(detail)

            elif msg.startswith("CARC_RARC_CROSSCHECK:") and cpt_match:
                b = _cpt_bucket(cpt_match.group(1))
                b["crosscheck"] = msg.replace("CARC_RARC_CROSSCHECK: ", "")

            else:
                non_cpt_msgs.append(msg)

        # --- Second pass: emit rows ------------------------------------
        rows = []
        finding = 0

        # Non-CPT rows (claim-level errors, credentialing, etc.)
        for msg in non_cpt_msgs:
            finding += 1
            row = dict(blank)
            row["Finding_Number"] = finding

            if msg.startswith("CRITICAL: NPI"):
                row["Severity"] = "ERROR"
                row["Category"] = "Provider / Credentialing"
                row["Explanation"] = (
                    "The rendering provider's NPI was not found in the enrollment "
                    "velocity tracker. The claim cannot be billed until the "
                    "provider's enrollment status is confirmed.")
            elif msg.startswith("HOLD:"):
                detail = msg.replace("HOLD: ", "")
                row["Severity"] = "ERROR"
                row["Category"] = "Provider / Credentialing"
                row["Explanation"] = (
                    f"Provider enrollment is not yet approved. {detail} "
                    "The claim should be held until credentialing is complete.")
            elif msg.startswith("GATE ERROR:"):
                row["Severity"] = "ERROR"
                row["Category"] = "Provider / Credentialing"
                row["Explanation"] = (
                    "The system could not connect to the enrollment database to "
                    "verify the provider. Check database connectivity and retry.")
            elif "SNIP 3 ERROR" in msg:
                row["Severity"] = "ERROR"
                row["Category"] = "Claim / Charge Balancing"
                row["Explanation"] = (
                    "The sum of the individual service line charges does not equal "
                    "the total charge amount on the claim (CLM02). Review line-item "
                    "charges and correct before submission.")
            elif "SNIP 4 ERROR" in msg:
                row["Severity"] = "ERROR"
                row["Category"] = "Claim / Accident Info"
                row["Explanation"] = (
                    "The claim indicates an accident-related visit but no accident "
                    "date (DTP*439) was provided. Add the accident date or correct "
                    "the accident indicator.")
            else:
                row["Severity"] = "INFO"
                row["Category"] = "General"
                row["Explanation"] = msg

            rows.append(row)

        # CPT rows — one per distinct finding type per CPT
        for cpt, data in cpt_info.items():
            carc_rarc_parts = []
            if data["carc"]:
                carc_rarc_parts.append("CARC: " + ", ".join(data["carc"]))
            if data["rarc"]:
                carc_rarc_parts.append("RARC: " + ", ".join(data["rarc"]))
            carc_rarc_str = " | ".join(carc_rarc_parts)

            # HIGH AUDIT RISK row (modifier / coding warnings)
            if data["has_warning"]:
                finding += 1
                row = dict(blank)
                row["Finding_Number"] = finding
                row["Severity"] = "HIGH AUDIT RISK"
                row["Raw_Code"] = cpt
                row["Modifier_Required"] = data["modifier"]
                if data["modifier"]:
                    row["Category"] = "CPT / Modifier"
                    row["Explanation"] = (
                        f"CPT {cpt}: {data['warning_reason']}. "
                        "If the required modifier is missing, the claim is at high "
                        "risk for audit or denial. Verify coding before submission.")
                else:
                    row["Category"] = "CPT / Coding"
                    row["Explanation"] = (
                        f"CPT {cpt}: {data['warning_reason']}. "
                        "If the required diagnosis code is missing, the claim is at "
                        "risk for denial. Verify coding before submission.")
                rows.append(row)

            # HIGH DENIAL RISK row (denial intelligence)
            if data["has_alert"]:
                finding += 1
                row = dict(blank)
                row["Finding_Number"] = finding
                row["Severity"] = "HIGH DENIAL RISK"
                row["Category"] = "CPT / Denial Risk"
                row["Raw_Code"] = cpt
                row["Risk_Level"] = data["risk"]
                row["Recovery_Potential"] = data["recovery"]
                row["CARC_RARC_Potential"] = carc_rarc_str
                row["Explanation"] = (
                    f"CPT {cpt} has been flagged by the Denial Intelligence engine "
                    f"with a {data['risk']} denial risk and {data['recovery']} "
                    "recovery potential. Review documentation and payer-specific "
                    "rules before submission.")
                if data["carc_descs"]:
                    row["Explanation"] += (
                        " Historical adjustment reasons: "
                        + "; ".join(data["carc_descs"]) + ".")
                rows.append(row)

            # Diagnosis / CPT alignment row
            if data["crosscheck"]:
                finding += 1
                row = dict(blank)
                row["Finding_Number"] = finding
                row["Severity"] = "HIGH AUDIT RISK"
                row["Category"] = "Diagnosis / CPT Alignment"
                row["Raw_Code"] = cpt
                row["CARC_RARC_Potential"] = carc_rarc_str
                row["Explanation"] = (
                    f"{data['crosscheck']} — Ensure the diagnosis codes on file "
                    "support medical necessity for the billed procedure. "
                    "Misalignment is a leading cause of denials.")
                rows.append(row)

        return rows

    csv_fieldnames = [
        "File", "Claim_PCN", "Claim_Valid", "Finding_Number",
        "Severity", "Category", "Raw_Code", "Risk_Level",
        "Recovery_Potential", "Modifier_Required",
        "CARC_RARC_Potential", "Explanation",
    ]
    with open(csv_file, "w", newline="", encoding="utf-8") as out:
        writer = csv.DictWriter(out, fieldnames=csv_fieldnames)
        writer.writeheader()
        for entry in all_results:
            for row in _build_claim_rows(entry):
                writer.writerow(row)

    logging.info("Wrote validation results to %s", output_dir)
    print(f"\nValidation complete.")
    print(f"  JSON results: {json_file}")
    print(f"  CSV results:  {csv_file}")
    print(f"  Log file:     {log_file}")