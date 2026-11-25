"""Column-level cleaning rules, written as Spark expressions (no Python UDFs, so they run at full speed).

Each source's `typed_*` function turns the array of raw string fields into typed, standardised columns.
"""
from pyspark.sql import Column
from pyspark.sql import functions as F

# US states, DC and territories. Anything else in a state field becomes NULL and is counted in the quality report.
US_STATES = (
    "AL AK AZ AR CA CO CT DE FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH NJ NM NY NC ND OH OK OR "
    "PA RI SC SD TN TX UT VT VA WA WV WI WY DC PR VI GU AS MP"
).split()

MIN_YEAR, MAX_YEAR = 1949, 2030   # plausible model years (NHTSA uses 9999 for unknown)


def blank_to_null(c: Column) -> Column:
    t = F.trim(c)
    return F.when(t == "", None).otherwise(t)


def clean_text(c: Column) -> Column:
    """Trim, collapse runs of whitespace, empty -> NULL."""
    return blank_to_null(F.regexp_replace(c, r"\s+", " "))


def clean_name(c: Column) -> Column:
    """Upper-case names (make, model, manufacturer) with single spaces."""
    return F.upper(clean_text(c))


def strip_html(c: Column) -> Column:
    """Recall texts after 2010 contain HTML tags and entities (build-log F3)."""
    c = F.regexp_replace(c, r"<[^>]{1,200}>", " ")
    for ent, ch in (("&amp;", "&"), ("&quot;", '"'), ("&#39;", "'"), ("&lt;", "<"), ("&gt;", ">"), ("&nbsp;", " ")):
        c = F.regexp_replace(c, ent, ch)
    return clean_text(c)


def to_date(c: Column) -> Column:
    """YYYYMMDD -> date. Invalid or placeholder values ('0', '00000000', 1801-01-01...) become NULL."""
    d = F.to_date(F.try_to_timestamp(blank_to_null(c), F.lit("yyyyMMdd")))
    return F.when(d.between(F.lit("1949-01-01").cast("date"), F.date_add(F.current_date(), 1)), d)


def to_int(c: Column) -> Column:
    return blank_to_null(c).try_cast("int")


def positive_or_null(c: Column) -> Column:
    """Since the 2021 NHTSA system change, unknown numbers are published as 0 (see CMPL.txt). Treat 0 as unknown."""
    v = to_int(c)
    return F.when(v > 0, v)


def yes_no(c: Column) -> Column:
    t = F.upper(F.trim(c))
    return F.when(t == "Y", True).when(t == "N", False)


def model_year(c: Column) -> Column:
    y = to_int(c)
    return F.when(y.between(MIN_YEAR, MAX_YEAR), y)


def component_path(c: Column) -> Column:
    """'STEERING: STEERING WHEEL/HANDLE BAR' -> 'STEERING:STEERING WHEEL/HANDLE BAR' (one canonical spelling)."""
    return F.upper(clean_text(F.regexp_replace(c, r"\s*:\s*", ":")))


def component_system(path: Column) -> Column:
    """Top level of the component hierarchy: 'FUEL SYSTEM, GASOLINE:DELIVERY:FUEL PUMP' -> 'FUEL SYSTEM, GASOLINE'."""
    return F.split(path, ":").getItem(0)


def us_state(c: Column) -> Column:
    t = F.upper(F.trim(c))
    return F.when(t.isin(US_STATES), t)


def row_hash(cols: list[str]) -> Column:
    """Fingerprint of the business columns, used to detect changed records between daily snapshots."""
    return F.sha2(F.concat_ws("␟", *[F.coalesce(F.col(c).cast("string"), F.lit("␀")) for c in cols]), 256)


# ------------------------------------------------------------------ per-source typing (input: column `f` = array<string>)
def _f(i: int) -> Column:
    return F.get(F.col("f"), i)      # NULL when the row has fewer fields (older file versions)


def typed_complaints() -> list[Column]:
    path = component_path(_f(11))
    return [
        clean_text(_f(0)).alias("cmplid"),
        clean_text(_f(1)).alias("odino"),
        clean_name(_f(2)).alias("manufacturer"),
        clean_name(_f(3)).alias("make"),
        clean_name(_f(4)).alias("model"),
        model_year(_f(5)).alias("model_year"),
        yes_no(_f(6)).alias("crash"),
        to_date(_f(7)).alias("fail_date"),
        yes_no(_f(8)).alias("fire"),
        F.coalesce(to_int(_f(9)), F.lit(0)).alias("injured"),
        F.coalesce(to_int(_f(10)), F.lit(0)).alias("deaths"),
        path.alias("component_path"),
        component_system(path).alias("component_system"),
        clean_name(_f(12)).alias("city"),
        us_state(_f(13)).alias("state"),
        F.upper(clean_text(_f(14))).alias("vin_prefix"),
        to_date(_f(15)).alias("added_date"),
        to_date(_f(16)).alias("received_date"),
        positive_or_null(_f(17)).alias("miles"),
        positive_or_null(_f(18)).alias("occurrences"),
        clean_text(_f(19)).alias("narrative"),
        clean_text(_f(20)).alias("complaint_type"),
        yes_no(_f(21)).alias("police_report"),
        to_date(_f(22)).alias("purchase_date"),
        yes_no(_f(23)).alias("original_owner"),
        yes_no(_f(24)).alias("anti_lock_brakes"),
        yes_no(_f(25)).alias("cruise_control"),
        positive_or_null(_f(26)).alias("cylinders"),
        clean_name(_f(27)).alias("drive_train"),
        clean_name(_f(28)).alias("fuel_system"),
        clean_name(_f(29)).alias("fuel_type"),
        clean_name(_f(30)).alias("transmission_type"),
        positive_or_null(_f(31)).alias("vehicle_speed"),
        clean_text(_f(32)).alias("tire_dot"),
        clean_text(_f(33)).alias("tire_size"),
        clean_name(_f(34)).alias("tire_location"),
        clean_name(_f(35)).alias("tire_fail_type"),
        yes_no(_f(36)).alias("original_equipment"),
        to_date(_f(37)).alias("manufacture_date"),
        clean_name(_f(38)).alias("seat_type"),
        clean_name(_f(39)).alias("restraint_type"),
        clean_name(_f(40)).alias("dealer_name"),
        clean_text(_f(41)).alias("dealer_phone"),
        clean_name(_f(42)).alias("dealer_city"),
        us_state(_f(43)).alias("dealer_state"),
        clean_text(_f(44)).alias("dealer_zip"),
        clean_name(_f(45)).alias("product_type"),
        yes_no(_f(46)).alias("repaired"),
        yes_no(_f(47)).alias("medical_attention"),
        yes_no(_f(48)).alias("vehicle_towed"),
        us_state(_f(49)).alias("incident_state"),
        clean_name(_f(50)).alias("vehicle_operator"),
    ]


def typed_recalls() -> list[Column]:
    path = component_path(_f(6))
    return [
        to_int(_f(0)).alias("record_id"),
        clean_text(_f(1)).alias("campaign_id"),
        clean_name(_f(2)).alias("make"),
        clean_name(_f(3)).alias("model"),
        model_year(_f(4)).alias("model_year"),
        clean_text(_f(5)).alias("mfr_campaign_id"),
        path.alias("component_path"),
        component_system(path).alias("component_system"),
        clean_name(_f(7)).alias("manufacturer"),
        to_date(_f(8)).alias("mfg_begin_date"),
        to_date(_f(9)).alias("mfg_end_date"),
        clean_text(_f(10)).alias("recall_type"),
        to_int(_f(11)).alias("potentially_affected"),
        to_date(_f(12)).alias("owner_notified_date"),
        clean_name(_f(13)).alias("influenced_by"),
        clean_name(_f(14)).alias("recalled_product_mfr"),
        to_date(_f(15)).alias("report_received_date"),
        to_date(_f(16)).alias("record_created_date"),
        clean_text(_f(17)).alias("regulation_part"),
        clean_text(_f(18)).alias("fmvss"),
        strip_html(_f(19)).alias("defect_summary"),
        strip_html(_f(20)).alias("consequence_summary"),
        strip_html(_f(21)).alias("corrective_action"),
        strip_html(_f(22)).alias("notes"),
        clean_text(_f(23)).alias("recalled_component_id"),
        clean_text(_f(24)).alias("mfr_component_name"),
        clean_text(_f(25)).alias("mfr_component_desc"),
        clean_text(_f(26)).alias("mfr_component_part_no"),
        yes_no(F.substring(_f(27), 1, 1)).alias("do_not_drive"),
        yes_no(F.substring(_f(28), 1, 1)).alias("park_outside"),
    ]


def typed_investigations() -> list[Column]:
    path = component_path(_f(4))
    return [
        clean_text(_f(0)).alias("action_number"),
        F.regexp_extract(clean_text(_f(0)), r"^([A-Z]+)", 1).alias("action_type"),
        clean_name(_f(1)).alias("make"),
        clean_name(_f(2)).alias("model"),
        model_year(_f(3)).alias("model_year"),
        path.alias("component_path"),
        component_system(path).alias("component_system"),
        clean_name(_f(5)).alias("manufacturer"),
        to_date(_f(6)).alias("opened_date"),
        to_date(_f(7)).alias("closed_date"),
        clean_text(_f(8)).alias("campaign_id"),
        strip_html(_f(9)).alias("subject"),
        strip_html(_f(10)).alias("summary"),
    ]


TYPED = {"complaints": typed_complaints, "recalls": typed_recalls, "investigations": typed_investigations}

# Business key of the typed table (investigations have no single id: one action covers many vehicles)
KEYS = {
    "complaints": ["cmplid"],
    "recalls": ["record_id"],
    # one investigation covers many vehicles, components, recall campaigns and sometimes manufacturers (build-log F4)
    "investigations": ["action_number", "make", "model", "model_year", "component_path", "campaign_id", "manufacturer"],
}

# Oldest file layout still accepted: complaints gained fields 50-51 in April 2026 (CMPL.txt change log #12)
MIN_FIELDS = {"complaints": 49, "recalls": 29, "investigations": 11}
