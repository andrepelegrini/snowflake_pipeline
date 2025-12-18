# Analytics Engineer Assessment — System Design & Architecture (Snowflake)

This repository implements a Snowflake-native ingestion + modeling pipeline for daily partner CSV drops. The design is built to handle missing headers, duplicates, late arrivals, and imperfect referential integrity, while producing BI-ready reporting marts.

---

## Goals

- Ingest daily CSV drops for:
  - `merchants_<YYYY-MM-DD>.csv`
  - `applications_<YYYY-MM-DD>.csv`
  - `disbursements_<YYYY-MM-DD>.csv`
  - `payments_<YYYY-MM-DD>.csv`
- Preserve raw inputs (append-only) with load metadata for auditability.
- Produce clean, typed, deduplicated staging tables.
- Model core analytics in a Kimball-style dimensional model:
  - SCD Type 2 Merchant Dimension (history preserved; current row flagged)
  - Fact tables at the natural grain (application, disbursement, payment)
- Publish marts/views for:
  - Processing monitoring (data ops)
  - Lending performance (business KPIs)
  - Risk monitoring (credit risk early warning)

---

## Source Data & File Conventions

### File naming
Each file name ends with a batch date suffix:
- `..._<YYYY-MM-DD>.csv`

This batch date is extracted from the filename and stored as `batch_date`. It represents the “partner snapshot date” for merchant attributes and the “delivery batch date” for transaction files.

### CSV headers
Headers **may or may not** exist. The pipeline must not rely on them.

---

## Architecture Overview

The pipeline follows a layered approach:

1. **RAW** — append-only landing tables (all columns as STRING)
2. **STG** — typed, validated, deduplicated “clean” tables + invalid quarantine
3. **CORE** — dimensional model (SCD2 dimension + fact tables)
4. **MARTS** — BI-ready report views/tables

### Why this layering?
- **RAW** preserves exactly what arrived and when (auditable).
- **STG** is where parsing, validation, and dedupe happen in a controlled way.
- **CORE** implements stable business entities and relationships (Kimball).
- **MARTS** exposes curated datasets for Tableau/Looker/BI consumers.

---

## Data Flow (High Level)

```mermaid
flowchart LR
  A[Partner CSV files<br/>inbox/] -->|PUT to stage + COPY| R[RAW<br/>append-only strings + metadata]
  R -->|Streams| S[STG<br/>typed + validated + deduped]
  S --> C[CORE<br/>DIM_MERCHANT SCD2<br/>FACT_*]
  C --> M[MARTS<br/>report views/tables]
  S --> Q[STG.INVALID_RECORDS]
  R --> AUD[META.FILE_LOAD_AUDIT]
  S --> DQ[META.DQ_RESULTS]
```

## Key Assumptions

1. batch_date is extracted from the filename suffix `YYYY-MM-DD`.

2. Merchant files represent the latest merchant attributes as-of batch_date.

3. “Default” is not explicitly provided; if needed for reporting, define a proxy:

    * Example proxy: a disbursement is “defaulted” if any payment has days_from_due >= 30

    * This threshold is configurable based on credit policy.

4. Portfolio balance is principal-only unless interest accrual rules are introduced:

    * `outstanding_principal = disbursed_amount - sum(payments)`

## Questions needing team clarification
1. What is the official definition of **default**?
    * Days Past Due threshold (30/60/90)? Charge-off? Restructuring?
2. Are there **multiple payments per disbursement** only (1:many), or can a payment be allocated across multiple disbursements?
3. Can **IDs change** (e.g. different payment_id) or are they immutable?
4. Are merchant files true **ful snapshots** or partial updates?
5. For reporting:
    * Shold KPIs be based on **event dates** (disbursement_date) or **processing dates** (batch_date)?
6. Which merchant attributes should be treated as slowly changing (SCD2) vs overwritten(SCD1)?
7. Any business rules around:
    * excluding test merchants,
    * handling refunds/reversals,
    * chargeback payments,
    * negative amounts or adjustments?

## Dashboard (Streamlit in Snowflake)

The analytics dashboard is implemented as a **Streamlit app hosted inside Snowflake**. It reads directly from the curated marts in `R2_LENDING.MARTS`:

- `MART_DAILY_OVERVIEW` — daily funnel KPIs (applications, approvals, approval rate, disbursed vs. paid)
- `MART_MERCHANT_PERFORMANCE_CURRENT` — merchant scorecard (current snapshot only)
- `MART_COLLECTIONS_AGING` — collections / delinquency proxy by DPD bucket

**App link:** https://app.snowflake.com/rptathp/jr59504/#/streamlit-apps/R2_LENDING.MARTS.Q6J8A4VVD6PCGA67

### Access & sharing
This Streamlit app is secured by Snowflake and requires a valid **Snowflake username/password** in the same account, with permissions to query `R2_LENDING.MARTS`.  
For reviewers without access, screenshots are provided below.

### Screenshots

**Overview**
![Dashboard overview](docs/screenshots/streamlit1.png)

**Merchant performance**
![Merchant performance](docs/screenshots/streamlit2.png)

**Collections aging**
![Collections aging](docs/screenshots/streamlit3.png)


