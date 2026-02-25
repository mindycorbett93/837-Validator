"""
Shared test data commons for all healthcare test file generators.
Contains practice-specific CPT/ICD-10 codes, provider info, payer info,
patient demographics, CARC/RARC codes, and helper functions.
"""
import random
import string
from datetime import datetime, timedelta

# ============================================================
# PRACTICE TYPES
# ============================================================
PRACTICE_TYPES = [
    "primary_care", "internal_medicine", "allergy_immunology",
    "orthopaedics", "cardiology", "behavioral_health",
    "radiology", "pathology", "gastroenterology",
    "ob_gyn", "dermatology", "anesthesiology",
    "urgent_care", "pediatrics", "clinical_laboratory"
]

PRACTICE_DISPLAY_NAMES = {
    "primary_care": "Primary Care",
    "internal_medicine": "Internal Medicine",
    "allergy_immunology": "Allergy and Immunology",
    "orthopaedics": "Orthopaedics/Orthopaedic Surgery",
    "cardiology": "Cardiology/Cardiovascular Surgery",
    "behavioral_health": "Behavioral Health",
    "radiology": "Radiology",
    "pathology": "Pathology",
    "gastroenterology": "Gastroenterology",
    "ob_gyn": "OB/GYN",
    "dermatology": "Dermatology",
    "anesthesiology": "Anesthesiology",
    "urgent_care": "Urgent Care",
    "pediatrics": "Pediatrics",
    "clinical_laboratory": "Clinical Laboratory"
}

PRACTICE_TAXONOMY_CODES = {
    "primary_care": "208D00000X",
    "internal_medicine": "207R00000X",
    "allergy_immunology": "207K00000X",
    "orthopaedics": "207X00000X",
    "cardiology": "207RC0000X",
    "behavioral_health": "103T00000X",
    "radiology": "2085R0202X",
    "pathology": "207ZP0102X",
    "gastroenterology": "207RG0100X",
    "ob_gyn": "207V00000X",
    "dermatology": "207N00000X",
    "anesthesiology": "207L00000X",
    "urgent_care": "261QU0200X",
    "pediatrics": "208000000X",
    "clinical_laboratory": "291U00000X"
}

# ============================================================
# SPECIALTY-SPECIFIC CPT CODES
# ============================================================
SPECIALTY_CPT_CODES = {
    "primary_care": [
        ("99213", "Office visit, established, low complexity", 95.00),
        ("99214", "Office visit, established, moderate complexity", 145.00),
        ("99215", "Office visit, established, high complexity", 210.00),
        ("99203", "Office visit, new patient, low complexity", 130.00),
        ("99204", "Office visit, new patient, moderate complexity", 200.00),
        ("99205", "Office visit, new patient, high complexity", 275.00),
        ("99385", "Preventive visit, new, 18-39", 200.00),
        ("99395", "Preventive visit, established, 18-39", 175.00),
        ("99396", "Preventive visit, established, 40-64", 190.00),
        ("G0438", "Annual wellness visit, initial", 175.00),
        ("G0439", "Annual wellness visit, subsequent", 120.00),
        ("99490", "Chronic care management, 20 min", 42.00),
        ("36415", "Venipuncture", 10.00),
        ("96372", "Therapeutic injection, SC/IM", 25.00),
        ("71046", "Chest X-ray, 2 views", 45.00),
    ],
    "internal_medicine": [
        ("99213", "Office visit, established, low complexity", 95.00),
        ("99214", "Office visit, established, moderate complexity", 145.00),
        ("99215", "Office visit, established, high complexity", 210.00),
        ("99204", "Office visit, new patient, moderate complexity", 200.00),
        ("99205", "Office visit, new patient, high complexity", 275.00),
        ("99223", "Hospital admit, high complexity", 350.00),
        ("99232", "Subsequent hospital care, moderate", 120.00),
        ("99233", "Subsequent hospital care, high", 175.00),
        ("99238", "Hospital discharge, 30 min or less", 150.00),
        ("93000", "EKG, 12-lead with interpretation", 35.00),
        ("80053", "Comprehensive metabolic panel", 25.00),
        ("85025", "CBC with differential", 15.00),
        ("99490", "Chronic care management", 42.00),
        ("99495", "Transitional care mgmt, 14-day", 250.00),
        ("99496", "Transitional care mgmt, 7-day", 320.00),
    ],
    "allergy_immunology": [
        ("99213", "Office visit, established, low complexity", 95.00),
        ("99214", "Office visit, established, moderate complexity", 145.00),
        ("99204", "Office visit, new patient, moderate complexity", 200.00),
        ("95004", "Percutaneous allergy skin test", 8.00),
        ("95024", "Intracutaneous allergy test", 12.00),
        ("95044", "Patch allergy test", 10.00),
        ("95115", "Allergen immunotherapy, single injection", 20.00),
        ("95117", "Allergen immunotherapy, 2+ injections", 35.00),
        ("95165", "Antigen therapy, single dose vial", 15.00),
        ("95180", "Rapid desensitization", 250.00),
        ("94010", "Spirometry", 40.00),
        ("94060", "Bronchospasm evaluation", 75.00),
        ("86003", "Allergen specific IgE", 18.00),
        ("86005", "Allergen specific IgE, qualitative", 15.00),
        ("94375", "Respiratory flow volume loop", 35.00),
    ],
    "orthopaedics": [
        ("99213", "Office visit, established, low complexity", 95.00),
        ("99214", "Office visit, established, moderate complexity", 145.00),
        ("27447", "Total knee arthroplasty", 1800.00),
        ("27130", "Total hip arthroplasty", 2000.00),
        ("29881", "Knee arthroscopy, meniscectomy", 950.00),
        ("29826", "Shoulder arthroscopy, decompression", 1100.00),
        ("20610", "Arthrocentesis, major joint", 125.00),
        ("20611", "Arthrocentesis with ultrasound", 155.00),
        ("73721", "MRI lower extremity joint", 450.00),
        ("73221", "MRI upper extremity joint", 425.00),
        ("27786", "Open treatment ankle fracture", 750.00),
        ("25600", "Closed treatment distal radius fracture", 450.00),
        ("20680", "Hardware removal, deep", 650.00),
        ("97110", "Therapeutic exercises", 42.00),
        ("97140", "Manual therapy techniques", 42.00),
    ],
    "cardiology": [
        ("99214", "Office visit, established, moderate complexity", 145.00),
        ("99215", "Office visit, established, high complexity", 210.00),
        ("99205", "Office visit, new patient, high complexity", 275.00),
        ("93000", "EKG, 12-lead", 35.00),
        ("93306", "Echocardiography, complete", 350.00),
        ("93350", "Stress echocardiography", 425.00),
        ("93017", "Cardiovascular stress test", 175.00),
        ("93458", "Left heart catheterization", 1200.00),
        ("92928", "Percutaneous coronary stent", 2500.00),
        ("33533", "CABG, single arterial graft", 5000.00),
        ("93279", "Pacemaker interrogation", 45.00),
        ("93453", "Combined heart catheterization", 1500.00),
        ("93880", "Duplex scan, extracranial arteries", 250.00),
        ("93971", "Duplex scan, extremity veins", 225.00),
        ("93798", "Cardiac rehabilitation", 75.00),
    ],
    "behavioral_health": [
        ("90791", "Psychiatric diagnostic evaluation", 250.00),
        ("90792", "Psych diagnostic eval with medical", 275.00),
        ("90834", "Psychotherapy, 45 minutes", 130.00),
        ("90837", "Psychotherapy, 60 minutes", 175.00),
        ("90832", "Psychotherapy, 30 minutes", 85.00),
        ("90847", "Family psychotherapy with patient", 150.00),
        ("90846", "Family psychotherapy without patient", 140.00),
        ("90853", "Group psychotherapy", 50.00),
        ("99213", "Office visit, established, low complexity", 95.00),
        ("99214", "Office visit, established, moderate complexity", 145.00),
        ("96127", "Brief emotional/behavioral assessment", 8.00),
        ("96130", "Psychological testing evaluation", 175.00),
        ("96131", "Psychological testing, additional hour", 150.00),
        ("90833", "Psychotherapy add-on, 30 min", 65.00),
        ("90836", "Psychotherapy add-on, 45 min", 100.00),
    ],
    "radiology": [
        ("71046", "Chest X-ray, 2 views", 45.00),
        ("71250", "CT chest without contrast", 350.00),
        ("71260", "CT chest with contrast", 425.00),
        ("72148", "MRI lumbar spine without contrast", 500.00),
        ("70553", "MRI brain with/without contrast", 650.00),
        ("74177", "CT abdomen/pelvis with contrast", 475.00),
        ("77067", "Screening mammography, bilateral", 175.00),
        ("76830", "Transvaginal ultrasound", 225.00),
        ("73721", "MRI lower extremity joint", 450.00),
        ("76942", "Ultrasound guidance, needle", 125.00),
        ("77386", "IMRT delivery, complex", 350.00),
        ("77014", "CT guidance, radiation therapy", 175.00),
        ("70551", "MRI brain without contrast", 475.00),
        ("73221", "MRI upper extremity joint", 425.00),
        ("74176", "CT abdomen/pelvis without contrast", 400.00),
    ],
    "pathology": [
        ("88305", "Surgical pathology, level IV", 125.00),
        ("88307", "Surgical pathology, level V", 200.00),
        ("88312", "Special stain, Group I", 85.00),
        ("88313", "Special stain, Group II", 100.00),
        ("88342", "Immunohistochemistry", 125.00),
        ("88360", "Morphometric analysis, tumor", 175.00),
        ("88341", "Immunohistochemistry, additional", 100.00),
        ("88173", "Cytopathology, fine needle aspirate", 150.00),
        ("88112", "Cytopathology, cell enhancement", 60.00),
        ("80053", "Comprehensive metabolic panel", 25.00),
        ("85025", "CBC with differential", 15.00),
        ("87086", "Urine culture", 18.00),
        ("88302", "Surgical pathology, level II", 75.00),
        ("88304", "Surgical pathology, level III", 100.00),
        ("88321", "Consultation and report", 175.00),
    ],
    "gastroenterology": [
        ("99214", "Office visit, established, moderate complexity", 145.00),
        ("99215", "Office visit, established, high complexity", 210.00),
        ("99205", "Office visit, new patient, high complexity", 275.00),
        ("43239", "Upper GI endoscopy with biopsy", 500.00),
        ("45380", "Colonoscopy with biopsy", 750.00),
        ("45385", "Colonoscopy with polyp removal", 850.00),
        ("43249", "Esophagogastroduodenoscopy, dilation", 600.00),
        ("43235", "Upper GI endoscopy, diagnostic", 425.00),
        ("91035", "Esophageal function test", 175.00),
        ("91034", "Esophageal reflux test", 200.00),
        ("74177", "CT abdomen/pelvis with contrast", 475.00),
        ("76700", "Abdominal ultrasound, complete", 175.00),
        ("45378", "Colonoscopy, diagnostic", 650.00),
        ("43247", "EGD with foreign body removal", 550.00),
        ("91010", "Esophageal motility study", 225.00),
    ],
    "ob_gyn": [
        ("99213", "Office visit, established, low complexity", 95.00),
        ("99214", "Office visit, established, moderate complexity", 145.00),
        ("99205", "Office visit, new patient, high complexity", 275.00),
        ("59400", "Routine OB care, vaginal delivery", 3500.00),
        ("59510", "Routine OB care, cesarean delivery", 4200.00),
        ("59025", "Fetal non-stress test", 125.00),
        ("76801", "OB ultrasound, first trimester", 225.00),
        ("76811", "OB ultrasound, detailed", 350.00),
        ("76817", "Transvaginal ultrasound, OB", 225.00),
        ("58661", "Laparoscopy, excision of lesions", 1200.00),
        ("58558", "Hysteroscopy with biopsy", 800.00),
        ("57454", "Colposcopy with biopsy", 300.00),
        ("88175", "Pap smear, liquid-based", 35.00),
        ("81002", "Urinalysis, non-automated", 8.00),
        ("99384", "Preventive visit, new, 12-17", 175.00),
    ],
    "dermatology": [
        ("99213", "Office visit, established, low complexity", 95.00),
        ("99214", "Office visit, established, moderate complexity", 145.00),
        ("99203", "Office visit, new patient, low complexity", 130.00),
        ("11102", "Tangential biopsy of skin", 125.00),
        ("11104", "Punch biopsy of skin", 150.00),
        ("17000", "Destruction, premalignant lesion, first", 75.00),
        ("17003", "Destruction, premalignant, 2nd-14th", 20.00),
        ("17110", "Destruction, benign lesions, up to 14", 100.00),
        ("11600", "Excision, malignant lesion, trunk", 275.00),
        ("11640", "Excision, malignant lesion, face", 325.00),
        ("96910", "Photochemotherapy", 125.00),
        ("96920", "Laser treatment, skin, first lesion", 200.00),
        ("10060", "Incision and drainage, abscess", 175.00),
        ("11400", "Excision, benign lesion, trunk", 200.00),
        ("17311", "Mohs surgery, first stage", 750.00),
    ],
    "anesthesiology": [
        ("00100", "Anesthesia, salivary gland", 450.00),
        ("00142", "Anesthesia, lens surgery", 400.00),
        ("00400", "Anesthesia, chest wall surgery", 600.00),
        ("00540", "Anesthesia, chest procedure", 750.00),
        ("00630", "Anesthesia, lumbar region", 550.00),
        ("00740", "Anesthesia, upper GI procedure", 600.00),
        ("00810", "Anesthesia, lower GI procedure", 575.00),
        ("01402", "Anesthesia, knee arthroplasty", 700.00),
        ("01480", "Anesthesia, lower leg surgery", 500.00),
        ("01961", "Anesthesia, cesarean delivery", 650.00),
        ("01967", "Neuraxial labor analgesia", 500.00),
        ("62323", "Lumbar epidural injection", 350.00),
        ("64483", "Transforaminal epidural injection", 400.00),
        ("64415", "Brachial plexus nerve block", 300.00),
        ("99140", "Anesthesia, emergency conditions", 150.00),
    ],
    "urgent_care": [
        ("99213", "Office visit, established, low complexity", 95.00),
        ("99214", "Office visit, established, moderate complexity", 145.00),
        ("99203", "Office visit, new patient, low complexity", 130.00),
        ("99204", "Office visit, new patient, moderate complexity", 200.00),
        ("99281", "ED visit, self-limited problem", 75.00),
        ("99282", "ED visit, low to moderate severity", 125.00),
        ("99283", "ED visit, moderate severity", 200.00),
        ("12001", "Simple wound repair, up to 2.5cm", 175.00),
        ("12002", "Simple wound repair, 2.6-7.5cm", 225.00),
        ("29125", "Forearm splint application", 85.00),
        ("71046", "Chest X-ray, 2 views", 45.00),
        ("87880", "Strep test, rapid", 18.00),
        ("87804", "Influenza assay with optics", 22.00),
        ("81002", "Urinalysis, non-automated", 8.00),
        ("36415", "Venipuncture", 10.00),
    ],
    "pediatrics": [
        ("99391", "Preventive visit, established, infant", 150.00),
        ("99392", "Preventive visit, established, 1-4", 160.00),
        ("99393", "Preventive visit, established, 5-11", 165.00),
        ("99394", "Preventive visit, established, 12-17", 175.00),
        ("99213", "Office visit, established, low complexity", 95.00),
        ("99214", "Office visit, established, moderate complexity", 145.00),
        ("99203", "Office visit, new patient, low complexity", 130.00),
        ("90460", "Immunization admin, first component", 30.00),
        ("90461", "Immunization admin, each additional", 15.00),
        ("90707", "MMR vaccine", 65.00),
        ("90715", "Tdap vaccine", 50.00),
        ("96110", "Developmental screening", 15.00),
        ("92551", "Audiometry screening", 20.00),
        ("99173", "Visual acuity screening", 10.00),
        ("87880", "Strep test, rapid", 18.00),
    ],
    "clinical_laboratory": [
        ("80053", "Comprehensive metabolic panel", 25.00),
        ("80048", "Basic metabolic panel", 18.00),
        ("85025", "CBC with differential", 15.00),
        ("85027", "CBC without differential", 12.00),
        ("81001", "Urinalysis, automated with micro", 10.00),
        ("82947", "Glucose, blood", 8.00),
        ("83036", "Hemoglobin A1c", 20.00),
        ("80061", "Lipid panel", 22.00),
        ("84443", "TSH", 25.00),
        ("87086", "Urine culture", 18.00),
        ("87491", "Chlamydia, nucleic acid", 35.00),
        ("86580", "TB skin test", 12.00),
        ("82306", "Vitamin D, 25-hydroxy", 45.00),
        ("84153", "PSA, total", 28.00),
        ("82607", "Vitamin B-12", 22.00),
    ]
}

# ============================================================
# SPECIALTY-SPECIFIC ICD-10 CODES
# ============================================================
SPECIALTY_ICD10_CODES = {
    "primary_care": [
        ("Z00.00", "Encounter for general adult medical exam"),
        ("J06.9", "Acute upper respiratory infection"),
        ("I10", "Essential hypertension"),
        ("E11.9", "Type 2 diabetes mellitus without complications"),
        ("E78.5", "Hyperlipidemia, unspecified"),
        ("J02.9", "Acute pharyngitis, unspecified"),
        ("M54.5", "Low back pain"),
        ("R05.9", "Cough, unspecified"),
        ("N39.0", "Urinary tract infection"),
        ("K21.0", "GERD with esophagitis"),
        ("F32.9", "Major depressive disorder, unspecified"),
        ("J45.909", "Unspecified asthma, uncomplicated"),
    ],
    "internal_medicine": [
        ("I10", "Essential hypertension"),
        ("E11.65", "Type 2 DM with hyperglycemia"),
        ("I25.10", "Atherosclerotic heart disease"),
        ("E78.5", "Hyperlipidemia"),
        ("J44.1", "COPD with acute exacerbation"),
        ("N18.3", "Chronic kidney disease, stage 3"),
        ("I48.91", "Atrial fibrillation, unspecified"),
        ("K70.30", "Alcoholic cirrhosis without ascites"),
        ("D64.9", "Anemia, unspecified"),
        ("E03.9", "Hypothyroidism, unspecified"),
        ("G47.33", "Obstructive sleep apnea"),
        ("I50.9", "Heart failure, unspecified"),
    ],
    "allergy_immunology": [
        ("J30.1", "Allergic rhinitis due to pollen"),
        ("J30.9", "Allergic rhinitis, unspecified"),
        ("J45.20", "Mild intermittent asthma, uncomplicated"),
        ("J45.30", "Mild persistent asthma, uncomplicated"),
        ("L20.9", "Atopic dermatitis, unspecified"),
        ("T78.40XA", "Allergy, unspecified, initial encounter"),
        ("T78.2XXA", "Anaphylactic shock, unspecified"),
        ("D69.0", "Allergic purpura"),
        ("L50.0", "Allergic urticaria"),
        ("T78.1XXA", "Other adverse food reactions"),
        ("J45.40", "Moderate persistent asthma, uncomplicated"),
        ("D89.9", "Immune disorder, unspecified"),
    ],
    "orthopaedics": [
        ("M17.11", "Primary osteoarthritis, right knee"),
        ("M16.11", "Primary osteoarthritis, right hip"),
        ("M23.211", "Derangement of anterior horn of medial meniscus"),
        ("M75.111", "Right rotator cuff tear"),
        ("S82.001A", "Fracture of right patella, initial"),
        ("S52.501A", "Fracture of lower end of radius, initial"),
        ("M54.5", "Low back pain"),
        ("M79.3", "Panniculitis, unspecified"),
        ("M25.561", "Pain in right knee"),
        ("M76.891", "Tendinitis, right lower leg"),
        ("M80.08XA", "Age-related osteoporosis with fracture"),
        ("S42.001A", "Fracture of right clavicle, initial"),
    ],
    "cardiology": [
        ("I25.10", "Atherosclerotic heart disease of native artery"),
        ("I48.91", "Atrial fibrillation, unspecified"),
        ("I50.9", "Heart failure, unspecified"),
        ("I21.09", "STEMI of anterior wall"),
        ("I10", "Essential hypertension"),
        ("I35.0", "Aortic valve stenosis"),
        ("I42.9", "Cardiomyopathy, unspecified"),
        ("I49.9", "Cardiac arrhythmia, unspecified"),
        ("I25.110", "Atherosclerotic heart disease with unstable angina"),
        ("I63.9", "Cerebral infarction, unspecified"),
        ("I70.0", "Atherosclerosis of aorta"),
        ("I26.99", "Other pulmonary embolism"),
    ],
    "behavioral_health": [
        ("F32.1", "Major depressive disorder, moderate"),
        ("F33.0", "Major depressive disorder, recurrent, mild"),
        ("F41.1", "Generalized anxiety disorder"),
        ("F41.0", "Panic disorder"),
        ("F43.10", "Post-traumatic stress disorder, unspecified"),
        ("F31.9", "Bipolar disorder, unspecified"),
        ("F90.9", "ADHD, unspecified"),
        ("F10.20", "Alcohol dependence, uncomplicated"),
        ("F11.20", "Opioid dependence, uncomplicated"),
        ("F50.00", "Anorexia nervosa, unspecified"),
        ("F42.9", "OCD, unspecified"),
        ("F84.0", "Autistic disorder"),
    ],
    "radiology": [
        ("R91.8", "Other nonspecific abnormal finding of lung"),
        ("Z12.31", "Encounter for screening mammogram"),
        ("M54.5", "Low back pain"),
        ("G43.909", "Migraine, unspecified"),
        ("S06.0X0A", "Concussion without loss of consciousness"),
        ("R10.9", "Unspecified abdominal pain"),
        ("C34.90", "Malignant neoplasm of bronchus/lung"),
        ("M79.3", "Panniculitis"),
        ("K80.20", "Gallstone without cholecystitis"),
        ("N63.0", "Unspecified lump in breast"),
        ("S72.001A", "Fracture of femur, initial"),
        ("R93.89", "Abnormal findings on diagnostic imaging"),
    ],
    "pathology": [
        ("C44.319", "Basal cell carcinoma of skin"),
        ("C50.919", "Malignant neoplasm of breast"),
        ("D05.10", "Intraductal carcinoma in situ"),
        ("C18.9", "Malignant neoplasm of colon"),
        ("C61", "Malignant neoplasm of prostate"),
        ("D25.9", "Leiomyoma of uterus"),
        ("K35.80", "Unspecified acute appendicitis"),
        ("D17.9", "Benign lipomatous neoplasm"),
        ("N60.19", "Diffuse cystic mastopathy"),
        ("C73", "Malignant neoplasm of thyroid"),
        ("D48.9", "Neoplasm of uncertain behavior"),
        ("R85.619", "Unspecified abnormal cytological findings"),
    ],
    "gastroenterology": [
        ("K21.0", "GERD with esophagitis"),
        ("K57.30", "Diverticulosis of large intestine"),
        ("K50.90", "Crohn's disease, unspecified"),
        ("K51.90", "Ulcerative colitis, unspecified"),
        ("K80.20", "Calculus of gallbladder without cholecystitis"),
        ("K74.60", "Unspecified cirrhosis of liver"),
        ("K25.9", "Gastric ulcer, unspecified"),
        ("Z12.11", "Encounter for screening for colorectal cancer"),
        ("K22.70", "Barrett's esophagus without dysplasia"),
        ("K76.0", "Fatty liver, not elsewhere classified"),
        ("K58.9", "Irritable bowel syndrome"),
        ("K86.1", "Other chronic pancreatitis"),
    ],
    "ob_gyn": [
        ("Z34.00", "Encounter for supervision of normal pregnancy"),
        ("O80", "Encounter for full-term uncomplicated delivery"),
        ("N92.0", "Excessive menstruation with regular cycle"),
        ("N80.0", "Endometriosis of uterus"),
        ("D25.9", "Leiomyoma of uterus"),
        ("N83.20", "Unspecified ovarian cysts"),
        ("Z30.09", "Encounter for contraceptive management"),
        ("O24.410", "Gestational diabetes in pregnancy"),
        ("R87.619", "Unspecified abnormal cytological findings, cervix"),
        ("N76.0", "Acute vaginitis"),
        ("Z01.419", "Encounter for gynecological exam"),
        ("O09.519", "Supervision of elderly primigravida"),
    ],
    "dermatology": [
        ("L40.0", "Psoriasis vulgaris"),
        ("L20.9", "Atopic dermatitis, unspecified"),
        ("C44.319", "Basal cell carcinoma of skin"),
        ("C44.91", "Unspecified malignant neoplasm of skin"),
        ("D22.9", "Melanocytic nevi, unspecified"),
        ("L70.0", "Acne vulgaris"),
        ("L82.1", "Other seborrheic keratosis"),
        ("L57.0", "Actinic keratosis"),
        ("B35.1", "Tinea unguium"),
        ("L02.91", "Cutaneous abscess, unspecified"),
        ("L30.9", "Dermatitis, unspecified"),
        ("D48.5", "Neoplasm of uncertain behavior of skin"),
    ],
    "anesthesiology": [
        ("M54.5", "Low back pain"),
        ("G89.29", "Other chronic pain"),
        ("M47.816", "Spondylosis with myelopathy, lumbar region"),
        ("M51.16", "Intervertebral disc degeneration, lumbar"),
        ("G89.4", "Chronic pain syndrome"),
        ("M79.3", "Panniculitis, unspecified"),
        ("T81.11XA", "Postprocedural hemorrhage"),
        ("M54.16", "Radiculopathy, lumbar region"),
        ("G43.909", "Migraine, unspecified"),
        ("R52", "Pain, unspecified"),
        ("M47.812", "Spondylosis with myelopathy, cervical"),
        ("S39.002A", "Injury of abdominal wall"),
    ],
    "urgent_care": [
        ("J06.9", "Acute upper respiratory infection"),
        ("J02.9", "Acute pharyngitis"),
        ("S61.019A", "Laceration of right hand"),
        ("S93.401A", "Sprain of right ankle"),
        ("N39.0", "Urinary tract infection"),
        ("J01.90", "Acute sinusitis, unspecified"),
        ("H66.90", "Otitis media, unspecified"),
        ("R50.9", "Fever, unspecified"),
        ("J20.9", "Acute bronchitis, unspecified"),
        ("T14.8", "Other injury of unspecified body region"),
        ("R05.9", "Cough, unspecified"),
        ("R10.9", "Unspecified abdominal pain"),
    ],
    "pediatrics": [
        ("Z00.129", "Encounter for routine child health exam"),
        ("J06.9", "Acute upper respiratory infection"),
        ("H66.90", "Otitis media, unspecified"),
        ("J02.9", "Acute pharyngitis"),
        ("J45.20", "Mild intermittent asthma"),
        ("R50.9", "Fever, unspecified"),
        ("L20.9", "Atopic dermatitis"),
        ("Z23", "Encounter for immunization"),
        ("R62.51", "Failure to thrive, child"),
        ("F90.9", "ADHD, unspecified type"),
        ("J20.9", "Acute bronchitis, unspecified"),
        ("B34.9", "Viral infection, unspecified"),
    ],
    "clinical_laboratory": [
        ("E11.9", "Type 2 DM without complications"),
        ("E78.5", "Hyperlipidemia, unspecified"),
        ("E03.9", "Hypothyroidism, unspecified"),
        ("D64.9", "Anemia, unspecified"),
        ("N39.0", "Urinary tract infection"),
        ("Z00.00", "General adult medical exam"),
        ("I10", "Essential hypertension"),
        ("E55.9", "Vitamin D deficiency"),
        ("R73.09", "Other abnormal glucose"),
        ("B20", "HIV disease"),
        ("N40.0", "BPH without lower urinary tract symptoms"),
        ("R79.89", "Other specified abnormal findings of blood"),
    ]
}

# ============================================================
# PATIENT DATA
# ============================================================
FIRST_NAMES_M = ["James", "Robert", "Michael", "William", "David", "Richard", "Joseph", "Thomas",
                 "Charles", "Christopher", "Daniel", "Matthew", "Anthony", "Mark", "Donald",
                 "Steven", "Paul", "Andrew", "Joshua", "Kenneth", "Kevin", "Brian", "George",
                 "Timothy", "Ronald", "Edward", "Jason", "Jeffrey", "Ryan", "Jacob"]

FIRST_NAMES_F = ["Mary", "Patricia", "Jennifer", "Linda", "Barbara", "Elizabeth", "Susan",
                 "Jessica", "Sarah", "Karen", "Lisa", "Nancy", "Betty", "Margaret", "Sandra",
                 "Ashley", "Dorothy", "Kimberly", "Emily", "Donna", "Michelle", "Carol",
                 "Amanda", "Melissa", "Deborah", "Stephanie", "Rebecca", "Sharon", "Laura", "Cynthia"]

LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
              "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
              "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
              "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson",
              "Walker", "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen",
              "Hill", "Flores", "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera"]

STREETS = ["Main St", "Oak Ave", "Maple Dr", "Cedar Ln", "Pine St", "Elm Blvd",
           "Washington Ave", "Park Dr", "Lake Rd", "Hill St", "Spring Ln", "Forest Dr",
           "River Rd", "Valley View Dr", "Sunset Blvd", "Highland Ave", "Church St",
           "Meadow Ln", "College Ave", "Market St"]

CITIES_STATES_ZIPS = [
    ("New York", "NY", "10001"), ("Los Angeles", "CA", "90001"), ("Chicago", "IL", "60601"),
    ("Houston", "TX", "77001"), ("Phoenix", "AZ", "85001"), ("Philadelphia", "PA", "19101"),
    ("San Antonio", "TX", "78201"), ("San Diego", "CA", "92101"), ("Dallas", "TX", "75201"),
    ("San Jose", "CA", "95101"), ("Austin", "TX", "73301"), ("Jacksonville", "FL", "32099"),
    ("Fort Worth", "TX", "76101"), ("Columbus", "OH", "43085"), ("Charlotte", "NC", "28201"),
    ("Indianapolis", "IN", "46201"), ("Seattle", "WA", "98101"), ("Denver", "CO", "80201"),
    ("Nashville", "TN", "37201"), ("Portland", "OR", "97201"),
]

# ============================================================
# PROVIDER DATA
# ============================================================
PROVIDER_FIRST_NAMES = ["Robert", "James", "Michael", "William", "David", "Sarah",
                        "Jennifer", "Maria", "Elizabeth", "Patricia", "Richard", "John",
                        "Thomas", "Angela", "Christine", "Daniel", "Stephen", "Karen",
                        "Rebecca", "Laura"]

PROVIDER_LAST_NAMES = ["Chen", "Patel", "Kim", "Nguyen", "Williams", "Garcia", "Smith",
                       "Johnson", "Brown", "Anderson", "Martinez", "Thompson", "Robinson",
                       "Clark", "Lewis", "Walker", "Hall", "Young", "Wright", "Scott"]

PROVIDER_SUFFIXES = ["MD", "DO", "MD", "MD", "DO", "MD", "MD FACS", "MD PhD", "DO FACP", "MD"]

# ============================================================
# PAYER DATA
# ============================================================
PAYERS = [
    {"id": "00001", "name": "Aetna", "payer_id": "60054", "plan_type": "PPO"},
    {"id": "00002", "name": "Blue Cross Blue Shield", "payer_id": "BCBS1", "plan_type": "HMO"},
    {"id": "00003", "name": "Cigna", "payer_id": "62308", "plan_type": "PPO"},
    {"id": "00004", "name": "UnitedHealthcare", "payer_id": "87726", "plan_type": "PPO"},
    {"id": "00005", "name": "Humana", "payer_id": "61101", "plan_type": "HMO"},
    {"id": "00006", "name": "Medicare Part B", "payer_id": "CMS", "plan_type": "Medicare"},
    {"id": "00007", "name": "Medicaid", "payer_id": "MDCD", "plan_type": "Medicaid"},
    {"id": "00008", "name": "Kaiser Permanente", "payer_id": "91051", "plan_type": "HMO"},
    {"id": "00009", "name": "Anthem", "payer_id": "ANTM1", "plan_type": "PPO"},
    {"id": "00010", "name": "Tricare", "payer_id": "99726", "plan_type": "Government"},
]

# ============================================================
# CARC CODES (from cpt_denial_intelligence.sql)
# ============================================================
CARC_CODES = {
    "4":   {"desc": "The procedure code is inconsistent with the modifier used", "type": "SOFT", "category": "Coding", "rate": 78.5, "dept": "Coding Review"},
    "11":  {"desc": "The diagnosis is inconsistent with the procedure", "type": "SOFT", "category": "Coding", "rate": 72.0, "dept": "Coding Review"},
    "16":  {"desc": "Claim/service lacks information needed for adjudication", "type": "SOFT", "category": "Information", "rate": 85.0, "dept": "Billing/AR"},
    "18":  {"desc": "Exact duplicate claim/service", "type": "SOFT", "category": "Duplicate", "rate": 45.0, "dept": "Duplicate Review"},
    "22":  {"desc": "This care may be covered by another payer per COB", "type": "SOFT", "category": "COB", "rate": 70.0, "dept": "Eligibility/COB"},
    "27":  {"desc": "Expenses incurred after coverage terminated", "type": "HARD", "category": "Eligibility", "rate": 30.0, "dept": "Eligibility/COB"},
    "29":  {"desc": "The time limit for filing has expired", "type": "HARD", "category": "Timely Filing", "rate": 15.0, "dept": "Appeals"},
    "31":  {"desc": "Patient cannot be identified as our insured", "type": "SOFT", "category": "Eligibility", "rate": 65.0, "dept": "Eligibility/COB"},
    "50":  {"desc": "These are non-covered services", "type": "SOFT", "category": "Non-Covered", "rate": 48.0, "dept": "Clinical Appeals"},
    "96":  {"desc": "Non-covered charge(s)", "type": "HARD", "category": "Non-Covered", "rate": 20.0, "dept": "Billing/AR"},
    "97":  {"desc": "The benefit for this service is included in payment for another service", "type": "SOFT", "category": "Bundling", "rate": 65.0, "dept": "Coding Review"},
    "109": {"desc": "Claim/service not covered by this payer/contractor", "type": "SOFT", "category": "Wrong Payer", "rate": 62.0, "dept": "Credentialing/Enrollment"},
    "167": {"desc": "This (these) diagnosis(es) is (are) not covered", "type": "SOFT", "category": "Medical Necessity", "rate": 60.0, "dept": "Coding Review"},
    "185": {"desc": "The rendering provider is not eligible to perform the service billed", "type": "SOFT", "category": "Provider", "rate": 58.0, "dept": "Credentialing/Enrollment"},
    "197": {"desc": "Precertification/authorization/notification absent", "type": "SOFT", "category": "Authorization", "rate": 42.0, "dept": "Authorization"},
    "222": {"desc": "Exceeds the contracted maximum number of days/units", "type": "HARD", "category": "Limits", "rate": 25.0, "dept": "Clinical Appeals"},
    "1":   {"desc": "Deductible amount", "type": "HARD", "category": "Patient Responsibility", "rate": 5.0, "dept": "Patient Accounts"},
    "2":   {"desc": "Coinsurance amount", "type": "HARD", "category": "Patient Responsibility", "rate": 5.0, "dept": "Patient Accounts"},
    "3":   {"desc": "Copay amount", "type": "HARD", "category": "Patient Responsibility", "rate": 5.0, "dept": "Patient Accounts"},
}

# Non-standard CARC codes that need mapping
NON_STANDARD_CARC_CODES = {
    "N001": {"desc": "Service not medically necessary per plan guidelines", "maps_to": "167", "standard_desc": "Medical necessity denial"},
    "N002": {"desc": "Provider not contracted with plan", "maps_to": "185", "standard_desc": "Provider eligibility"},
    "N003": {"desc": "Authorization not obtained within required timeframe", "maps_to": "197", "standard_desc": "Authorization/precertification"},
    "N004": {"desc": "Claim submitted past deadline", "maps_to": "29", "standard_desc": "Timely filing"},
    "N005": {"desc": "Patient coverage inactive at time of service", "maps_to": "27", "standard_desc": "Coverage terminated"},
    "N006": {"desc": "Duplicate submission detected by payer system", "maps_to": "18", "standard_desc": "Duplicate claim"},
    "N007": {"desc": "Incorrect modifier used for procedure", "maps_to": "4", "standard_desc": "Modifier error"},
    "N008": {"desc": "Service bundled with primary procedure", "maps_to": "97", "standard_desc": "Bundling"},
    "N009": {"desc": "Patient responsibility - deductible not met", "maps_to": "1", "standard_desc": "Deductible"},
    "N010": {"desc": "Service exceeds plan benefit limits", "maps_to": "222", "standard_desc": "Benefit limits exceeded"},
}

# ============================================================
# RARC CODES
# ============================================================
RARC_CODES = {
    "N362": {"desc": "Missing/incomplete/invalid demo info", "type": "Alert"},
    "N386": {"desc": "This decision was based on a Local Coverage Determination", "type": "Alert"},
    "N657": {"desc": "This should be billed to the other payer first", "type": "Alert"},
    "MA130": {"desc": "Your claim contains incomplete or invalid info", "type": "Supplemental"},
    "N479": {"desc": "Missing/incomplete/invalid charge amount", "type": "Alert"},
    "N657": {"desc": "Bill other payer first", "type": "Alert"},
    "M15":  {"desc": "Separately billed services/tests denied", "type": "Supplemental"},
    "N381": {"desc": "Alert: Consult our website for fee schedule", "type": "Alert"},
    "MA04": {"desc": "Secondary payment cannot be considered", "type": "Supplemental"},
    "N432": {"desc": "Alert: Adjustment based on prior payer's allowance", "type": "Alert"},
    "N95":  {"desc": "This provider type/provider not eligible for this service", "type": "Supplemental"},
}

# ============================================================
# FACILITY DATA
# ============================================================
FACILITY_NAMES = {
    "system_a": [
        "Metro Health Medical Center", "Community General Hospital",
        "Valley Medical Associates", "Regional Healthcare System",
        "University Medical Partners"
    ],
    "system_b": [
        "Summit Health Network", "Pacific Care Medical Group",
        "Atlantic Health Partners", "Midwest Clinical Services",
        "National Health Alliance"
    ],
    "system_c": [
        "Heritage Health System", "Premier Medical Associates",
        "Advanced Care Network", "Integrated Health Services",
        "Pinnacle Healthcare Group"
    ]
}

FACILITY_NPI_BASE = {"system_a": 1234500000, "system_b": 5678900000, "system_c": 9012300000}

# ============================================================
# HELPER FUNCTIONS
# ============================================================

def generate_patient_id(seed):
    """Generate consistent patient ID from seed."""
    random.seed(seed)
    return f"PAT{random.randint(100000, 999999)}"

def generate_member_id(seed):
    """Generate insurance member ID."""
    random.seed(seed + 1000)
    prefix = random.choice(["MEM", "INS", "GRP", "PLN"])
    return f"{prefix}{random.randint(10000000, 99999999)}"

def generate_npi(base_offset):
    """Generate NPI number."""
    return f"{1000000000 + base_offset}"

def generate_claim_id(practice_idx, file_idx, line_idx=0):
    """Generate unique claim ID."""
    return f"CLM{practice_idx:02d}{file_idx:03d}{line_idx:02d}"

def generate_control_number(prefix, idx):
    """Generate EDI control number."""
    return f"{prefix}{idx:09d}"

def get_random_patient(seed):
    """Generate random patient demographics."""
    random.seed(seed)
    gender = random.choice(["M", "F"])
    if gender == "M":
        first = random.choice(FIRST_NAMES_M)
    else:
        first = random.choice(FIRST_NAMES_F)
    last = random.choice(LAST_NAMES)
    dob = datetime(random.randint(1940, 2005), random.randint(1, 12), random.randint(1, 28))
    street_num = random.randint(100, 9999)
    street = random.choice(STREETS)
    city, state, zipcode = random.choice(CITIES_STATES_ZIPS)
    ssn = f"{random.randint(100,999)}{random.randint(10,99)}{random.randint(1000,9999)}"
    phone = f"{random.randint(200,999)}{random.randint(200,999)}{random.randint(1000,9999)}"
    member_id = generate_member_id(seed)
    return {
        "first": first, "last": last, "gender": gender,
        "dob": dob.strftime("%Y%m%d"),
        "dob_hl7": dob.strftime("%Y%m%d"),
        "ssn": ssn, "phone": phone,
        "street": f"{street_num} {street}",
        "city": city, "state": state, "zip": zipcode,
        "member_id": member_id,
        "patient_id": generate_patient_id(seed),
    }

def get_random_provider(specialty, seed):
    """Generate random provider for a specialty."""
    random.seed(seed)
    first = random.choice(PROVIDER_FIRST_NAMES)
    last = random.choice(PROVIDER_LAST_NAMES)
    suffix = random.choice(PROVIDER_SUFFIXES)
    npi = generate_npi(seed % 50000)
    taxonomy = PRACTICE_TAXONOMY_CODES.get(specialty, "208D00000X")
    return {
        "first": first, "last": last, "suffix": suffix,
        "npi": npi, "taxonomy": taxonomy,
        "full_name": f"{first} {last}, {suffix}",
    }

def get_random_payer(seed):
    """Get random payer."""
    random.seed(seed)
    return random.choice(PAYERS)

def get_random_date(start_days_ago=365, end_days_ago=1, seed=None):
    """Generate random date in range."""
    if seed is not None:
        random.seed(seed)
    days_ago = random.randint(end_days_ago, start_days_ago)
    d = datetime.now() - timedelta(days=days_ago)
    return d

def format_date_edi(d):
    """Format date for EDI: YYYYMMDD"""
    return d.strftime("%Y%m%d")

def format_datetime_hl7(d):
    """Format datetime for HL7: YYYYMMDDHHMMSS"""
    return d.strftime("%Y%m%d%H%M%S")

def format_datetime_edi(d):
    """Format datetime for EDI segments."""
    return d.strftime("%Y%m%d"), d.strftime("%H%M")

def random_amount(low, high, seed=None):
    """Generate random dollar amount."""
    if seed is not None:
        random.seed(seed)
    return round(random.uniform(low, high), 2)

def get_specialty_codes(specialty, seed, num_codes=3):
    """Get random CPT and ICD-10 codes for specialty."""
    random.seed(seed)
    cpts = random.sample(SPECIALTY_CPT_CODES.get(specialty, SPECIALTY_CPT_CODES["primary_care"]),
                         min(num_codes, len(SPECIALTY_CPT_CODES.get(specialty, []))))
    icds = random.sample(SPECIALTY_ICD10_CODES.get(specialty, SPECIALTY_ICD10_CODES["primary_care"]),
                         min(num_codes, len(SPECIALTY_ICD10_CODES.get(specialty, []))))
    return cpts, icds
