ReadMe Summary: This module serves as the "Universal Adapter" for the RCM ecosystem. It automates the extraction of 837 claim files from EHRs and Clearinghouses via SFTP/API and utilizes Computer Vision (OCR) to digitize paper claims for pre-submission scrubbing.
1. Automated 837 Extraction (EHR & Clearinghouse)
Instead of manual downloads, we use a secure listener or API client to "pull" or "receive" 837 batches.
•	SFTP Listener: Monitors "Outbound" folders in the EHR for new 837P/I/D files.
•	API Connector: Interacts with clearinghouse endpoints (e.g., Availity or Waystar APIs) to fetch claim status or raw 837 data for re-validation.
2. Paper Claim Digitalization (OCR to 837)
For facilities still generating paper CMS-1500 or UB-04 forms, we build a bridge to convert images into actionable data.
•	Vision Pipeline: Uses OCR to map physical form fields (e.g., Box 24J for NPI) into a structured JSON format.
•	837 Generator: Re-assembles that JSON into an ANSI-standard 837 file so it can pass through your NPPES & Denial Validator.
