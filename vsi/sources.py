"""Source registry: where each NHTSA file lives and what its columns are.

Column names and order come from NHTSA's own field definitions (CMPL.txt, RCL.txt, INV.txt, downloaded with the
data into data/landing/<date>/). All files are tab-delimited, no header, no quoting, dates as YYYYMMDD.
"""
from dataclasses import dataclass, field

BASE = "https://static.nhtsa.gov/odi/ffdd"


@dataclass(frozen=True)
class Source:
    name: str                 # logical dataset name, used for table names
    files: tuple              # zip files under BASE (several zips can feed one dataset)
    columns: tuple            # column names in file order
    key: tuple                # business key used for dedupe and change detection
    doc: str                  # NHTSA's field-definition file
    description: str = ""
    extra: dict = field(default_factory=dict)


COMPLAINTS = Source(
    name="complaints",
    files=("cmpl/FLAT_CMPL.zip",),
    doc="cmpl/CMPL.txt",
    key=("cmplid",),
    description="Consumer vehicle safety complaints received by NHTSA since 1995. One row per complaint x component.",
    columns=(
        "cmplid", "odino", "mfr_name", "maketxt", "modeltxt", "yeartxt", "crash", "faildate", "fire", "injured",
        "deaths", "compdesc", "city", "state", "vin", "datea", "ldate", "miles", "occurences", "cdescr", "cmpl_type",
        "police_rpt_yn", "purch_dt", "orig_owner_yn", "anti_brakes_yn", "cruise_cont_yn", "num_cyls", "drive_train",
        "fuel_sys", "fuel_type", "trans_type", "veh_speed", "dot", "tire_size", "loc_of_tire", "tire_fail_type",
        "orig_equip_yn", "manuf_dt", "seat_type", "restraint_type", "dealer_name", "dealer_tel", "dealer_city",
        "dealer_state", "dealer_zip", "prod_type", "repaired_yn", "medical_attn", "vehicles_towed_yn",
        "state_of_incident", "vehicle_operator",
    ),
)

RECALLS = Source(
    name="recalls",
    files=("rcl/FLAT_RCL_PRE_2010.zip", "rcl/FLAT_RCL_POST_2010.zip"),
    doc="rcl/RCL.txt",
    key=("record_id",),
    description="Safety recall campaigns since 1967. One row per campaign x make/model/year x component.",
    columns=(
        "record_id", "campno", "maketxt", "modeltxt", "yeartxt", "mfgcampno", "compname", "mfgname", "bgman", "endman",
        "rcltypecd", "potaff", "odate", "influenced_by", "mfgtxt", "rcdate", "datea", "rpno", "fmvss", "desc_defect",
        "consequence_defect", "corrective_action", "notes", "rcl_cmpt_id", "mfr_comp_name", "mfr_comp_desc",
        "mfr_comp_ptno", "do_not_drive", "park_outside",
    ),
)

INVESTIGATIONS = Source(
    name="investigations",
    files=("inv/FLAT_INV.zip",),
    doc="inv/INV.txt",
    key=("nhtsa_action_number", "make", "model", "year", "compname"),
    description="Defect investigations opened by NHTSA since 1972. One row per investigation x make/model/year x component.",
    columns=("nhtsa_action_number", "make", "model", "year", "compname", "mfr_name", "odate", "cdate", "campno",
             "subject", "summary"),
)

ALL = {s.name: s for s in (COMPLAINTS, RECALLS, INVESTIGATIONS)}
