# Data model

Three layers (medallion). Bronze keeps every line exactly as received; silver is typed, cleaned current state with
change history; gold is a star schema plus data-product marts, built from SQL in `sql/gold/`.

## Star schema (gold)

```mermaid
erDiagram
    dim_vehicle ||--o{ fact_complaint : "vehicle_key"
    dim_component ||--o{ fact_complaint : "component_key"
    dim_manufacturer ||--o{ fact_complaint : "manufacturer_key"
    dim_state ||--o{ fact_complaint : "state_key"
    dim_date ||--o{ fact_complaint : "received_date_key / fail_date_key"
    dim_recall_campaign ||--o{ fact_recall_vehicle : "campaign_id"
    dim_vehicle ||--o{ fact_recall_vehicle : "vehicle_key"
    dim_component ||--o{ fact_recall_vehicle : "component_key"
    dim_vehicle ||--o{ fact_investigation : "vehicle_key"
    dim_component ||--o{ fact_investigation : "component_key"
    dim_recall_campaign ||--o{ fact_investigation : "campaign_id (led to)"
    dim_manufacturer ||--o{ dim_recall_campaign : "manufacturer_key"

    fact_complaint {
        string complaint_component_id PK "NHTSA CMPLID"
        string complaint_id "ODINO (degenerate dim)"
        bigint vehicle_key FK
        bigint component_key FK
        int received_date_key FK
        int crash_flag
        int fire_flag
        int injured
        int deaths
        int miles_at_failure
    }
    dim_recall_campaign {
        string campaign_id PK
        int potentially_affected "additive ONLY here"
        date report_received_date
        string influenced_by "MFR / ODI / OVSC"
    }
    fact_recall_vehicle {
        int recall_record_id PK
        string campaign_id FK
        bigint vehicle_key FK
        bigint component_key FK
    }
    fact_investigation {
        string action_number "PE / EA / DP / RQ ..."
        bigint vehicle_key FK
        string campaign_id "recall it led to"
        int days_open
    }
    dim_vehicle {
        bigint vehicle_key PK
        string make
        string model
        int model_year "NULL = unknown"
    }
    dim_component {
        bigint component_key PK
        string component_path "SYSTEM:SUB:PART"
        string component_system
        string component_subsystem
    }
```

## Grain decisions (the part interviewers ask about)

| Table | One row is | Why |
|---|---|---|
| `fact_complaint` | a complaint **x component** (CMPLID) | NHTSA files one row per component named. A complaint can name up to 65. Counting complaints = `COUNT(DISTINCT complaint_id)`, defined once in `metrics.yml`. |
| `dim_recall_campaign` | a recall campaign | "Potentially affected units" is a campaign-level number repeated on every raw row. Summing the raw rows would over-count (a campaign covering 40 vehicle rows counts its units 40 times). |
| `fact_recall_vehicle` | campaign x vehicle x component | What each recall covers. No unit counts here on purpose. |
| `fact_investigation` | investigation x vehicle x component x resulting campaign x manufacturer | Found by profiling (build-log F4): one investigation (e.g. Takata air bags, EA15001) led to 194 campaigns. |
| `mart_component_risk_monthly` | make x component system x month, **including zero months** | A baseline computed only over months with complaints would be biased upward and hide spikes. |
| `mart_recall_early_warning` | a vehicle recall campaign (2000+) | The data product's question is asked per recall. |

## Keys

- **Surrogate keys are hashes of the natural key** (`xxhash64(make, model, model_year)`), not sequences. Gold is fully
  rebuilt every run, and hash keys come out identical every time, so facts and dimensions can be rebuilt independently
  and keys stay stable across days (ADR-004).
- **Unknown members** (key -1) in every dimension, so facts never have NULL foreign keys and inner joins never silently drop rows.
- `dim_date` uses a readable `YYYYMMDD` integer key; it is a role-playing dimension (received, failure, report, opened, closed).

## Slowly changing data

- **silver.<source>**: current state. Each day's full snapshot is compared to it by business key and row hash
  -> inserts, updates, **soft deletes** (`_is_deleted`, `_deleted_load_date`). Every change is logged in `<source>_changes`.
- **silver.recalls_history**: SCD type 2 (`valid_from`, `valid_to`, `is_current`) for recall records, so you can ask
  "what did this recall say on a given date?" (NHTSA revises descriptions and unit counts after the first filing).
- Complaints keep current state + change log (not full SCD2): history of a consumer's complaint text has little
  analytical value, and SCD2 on 2.3M rows would double storage for no question anyone asks.
