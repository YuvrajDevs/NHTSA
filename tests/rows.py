"""Builders for raw NHTSA-style tab-separated lines, so tests read like the real files."""
from vsi.sources import ALL


def line(source: str, **fields) -> str:
    cols = ALL[source].columns
    vals = ["" for _ in cols]
    for k, v in fields.items():
        vals[cols.index(k)] = str(v)
    return "\t".join(vals)


def complaint(cmplid, odino="1000", make="FORD", model="F-150", year="2015", comp="SERVICE BRAKES, HYDRAULIC",
              ldate="20240115", faildate="20240110", text="BRAKES FAILED", **kw):
    return line("complaints", cmplid=cmplid, odino=odino, mfr_name="FORD MOTOR COMPANY", maketxt=make, modeltxt=model,
                yeartxt=year, crash="N", faildate=faildate, fire="N", injured=0, deaths=0, compdesc=comp, state="TX",
                ldate=ldate, datea=ldate, cdescr=text, cmpl_type="IVOQ", **kw)


def investigation(action, make="FORD", model="F-150", year="2015", comp="AIR BAGS", campno=""):
    return line("investigations", nhtsa_action_number=action, make=make, model=model, year=year, compname=comp,
                mfr_name="FORD MOTOR COMPANY", odate="20200101", cdate="20200601", campno=campno, subject="S", summary="X")


def recall(record_id, campno="24V100000", make="FORD", model="F-150", year="2015", comp="SERVICE BRAKES, HYDRAULIC",
           potaff="50000", rcdate="20240301"):
    return line("recalls", record_id=record_id, campno=campno, maketxt=make, modeltxt=model, yeartxt=year,
                compname=comp, mfgname="FORD MOTOR COMPANY", rcltypecd="V", potaff=potaff, influenced_by="MFR",
                rcdate=rcdate, datea=rcdate, desc_defect="<p>Brake line may leak</p>")
