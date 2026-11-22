# Telco dbt — Bronze → Silver → Gold on `TELCO_PROD_XL_V2`

Medallion pipeline for the Philippine telco dataset in `TELCO_PROD_XL_V2`, replacing the
stored-procedure pipelines in `PIPE` / `PIPE_X`.

```
BRONZE    TELCO_PROD_XL_V2.{CORE,BILLING,USAGE,CARE,NETWORK,MARKETING,PARTNER,REF,LANDING}
   │        63 source tables (dbt sources, read-only) ▸ freshness checks on CDR / xDR feeds
SILVER    TELCO_PROD_XL_V2.DBT_TELCO_SILVER    71 models — cleaned, deduped, typed, timezone-normalised, one rule-set (+16 insight intermediates)
   │        ▸ CDC from bronze change tracking: only inserted / updated / deleted rows since the last run
   │        ▸ int_subscriber_profile = the ONE place region / plan / brand / churn band are resolved
SNAPSHOTS TELCO_PROD_XL_V2.DBT_TELCO_SNAPSHOTS   6 SCD2 snapshots
GOLD      TELCO_PROD_XL_V2.DBT_TELCO_GOLD  150 models — 18 dims · 12 facts · 8 KPI marts · 29 aggregates · 31 analytics · 19 advanced · 33 insights (P01–P12)
```

---

## Setup after cloning — step by step

### 0. Prerequisites

| Need | Notes |
|---|---|
| Git | to clone the repo |
| **Python 3.10 – 3.12** | dbt 1.12 is tested on these; newer Pythons (3.13+) can fail to install some dependencies |
| OpenSSL | for generating the Snowflake key pair (pre-installed on macOS / Linux) |
| Snowflake user | with a role that can **read** `TELCO_PROD_XL_V2` and **create schemas** in a target database (step 3) |

### 1. Clone the repository

```bash
git clone https://github.com/vipintalreja-devx/dbt_core_snowflake.git
cd dbt_core_snowflake
```

### 2. Create a virtual environment and install dbt

```bash
python3.12 -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install --upgrade pip
pip install "dbt-core~=1.12.0" "dbt-snowflake~=1.12.0"
dbt --version                        # expect Core 1.12.x, snowflake plugin 1.12.x
```

### 3. Snowflake access (ask your Snowflake admin to run once)

The project reads bronze from `TELCO_PROD_XL_V2` and writes silver / gold / snapshots into the **same database**
as the schemas `DBT_TELCO_SILVER`, `DBT_TELCO_GOLD` and `DBT_TELCO_SNAPSHOTS`. Replace `<YOUR_USER>` below:

```sql
USE ROLE ACCOUNTADMIN;
CREATE ROLE IF NOT EXISTS DBT_ROLE;
GRANT ROLE DBT_ROLE TO USER <YOUR_USER>;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE DBT_ROLE;

-- read bronze + write DBT_TELCO_* schemas in the same database
USE ROLE SYSADMIN;                      -- owner of TELCO_PROD_XL_V2
GRANT USAGE, CREATE SCHEMA ON DATABASE TELCO_PROD_XL_V2 TO ROLE DBT_ROLE;
GRANT USAGE ON ALL SCHEMAS IN DATABASE TELCO_PROD_XL_V2 TO ROLE DBT_ROLE;
GRANT SELECT ON ALL TABLES IN DATABASE TELCO_PROD_XL_V2 TO ROLE DBT_ROLE;
GRANT SELECT ON ALL VIEWS  IN DATABASE TELCO_PROD_XL_V2 TO ROLE DBT_ROLE;
GRANT SELECT ON FUTURE TABLES IN DATABASE TELCO_PROD_XL_V2 TO ROLE DBT_ROLE;

-- dynamic tables (anl_ticket_backlog, anl_cell_daily_dt, adv_cell_hourly_state)
-- (DBT_ROLE owns the DBT_TELCO_* schemas it creates, so it can create dynamic tables in them)
```

### 4. Key-pair authentication (recommended)

Browser SSO (`authenticator: externalbrowser`) only works if the account has a SAML IdP configured —
otherwise it fails with error `390190`. Use key-pair auth instead:

```bash
mkdir -p ~/.ssh/snowflake && chmod 700 ~/.ssh/snowflake
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out ~/.ssh/snowflake/rsa_key.p8 -nocrypt
openssl rsa -in ~/.ssh/snowflake/rsa_key.p8 -pubout -out ~/.ssh/snowflake/rsa_key.pub
chmod 600 ~/.ssh/snowflake/rsa_key.p8
# print the public key body to paste into Snowflake:
grep -v -- '-----' ~/.ssh/snowflake/rsa_key.pub | tr -d '\n'; echo
```

In Snowflake (as an admin or the user's owner):

```sql
ALTER USER <YOUR_USER> SET RSA_PUBLIC_KEY='<paste the key body here>';
```

Never commit the `.p8` file.

### 5. Create the dbt profile

dbt looks for `~/.dbt/profiles.yml`. The profile **must be named `my_project`** (that is what
`dbt_project.yml` references). Create the file with:

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <ORGNAME-ACCOUNTNAME>        # e.g. BQYBJZW-XH07906 (Snowsight > account menu > account identifier)
      user: <YOUR_USER>
      authenticator: snowflake_jwt
      private_key_path: ~/.ssh/snowflake/rsa_key.p8
      role: DBT_ROLE
      warehouse: COMPUTE_WH
      database: TELCO_PROD_XL_V2             # silver / gold are written next to bronze
      schema: DBT_TELCO                      # -> DBT_TELCO_SILVER, DBT_TELCO_GOLD (+ DBT_TELCO_SNAPSHOTS)
      threads: 4
```

### 6. Verify the connection

```bash
dbt debug          # must end with "All checks passed!"
```

### 7. Install packages and build

```bash
dbt deps           # installs dbt_utils
dbt build          # seed + all models + snapshots + tests, in dependency order
```

> Do **not** run `dbt snapshot` on its own before the first `dbt build`: four snapshots
> (`snap_contract_profile`, `snap_subscriber_device`, `snap_cell_config`, `snap_account_profile`) read
> silver models, which must exist first. `dbt build` runs every snapshot at the right point in the DAG.

On an X-Small warehouse, with the default dev window (`history_start_date = 2026-08-01`), the first build
takes ~10 minutes and should end with:

```
Done. PASS=649 WARN=0 ERROR=0 SKIP=0   # 172 models + 6 snapshots + 1 seed + 470 tests
```

### 8. Explore

```bash
dbt docs generate && dbt docs serve     # lineage graph + column docs in the browser
dbt source freshness                    # bronze feed lag (warns: source data ends 2026-09-30)
```

### Troubleshooting

| Symptom | Fix |
|---|---|
| `390190 ... SAML Identity Provider` | account has no SSO — use key-pair auth (step 4) |
| `Could not find profile named 'my_project'` | profile name in `~/.dbt/profiles.yml` must be `my_project` |
| `JWT token is invalid` | public key not set on the user, or wrong user/account in profile |
| `Object 'TELCO_PROD_XL_V2...' does not exist or not authorized` | missing read grants (step 3) |
| `Insufficient privileges to operate on database 'TELCO_PROD_XL_V2'` | DBT_ROLE needs `CREATE SCHEMA` on the database (step 3) |
| dynamic table refresh fails | the owner role needs the grants itself (dynamic tables refresh as their owner) |
| build slow / queued | warehouse is shared; build one layer at a time: `dbt build -s tag:silver`, then `tag:gold` |

---

## Run (day to day)

```bash
source .venv/bin/activate
dbt build                      # daily: incremental models + snapshots + tests, in DAG order
```

Useful selections:

| Command | What it builds |
|---|---|
| `dbt build -s tag:silver` | silver layer only |
| `dbt build -s tag:gold` | gold layer only |
| `dbt build -s tag:advanced` / `tag:analytics` / `tag:aggregates` | one gold family |
| `dbt build -s tag:h05` | one ported model by its original id |
| `dbt build -s tag:billing` | everything tagged with a domain |
| `dbt build -s fct_subscriber_monthly+` | a model and everything downstream |
| `dbt build -s slv_cdr_voice --full-refresh` | rebuild one incremental model from scratch |

### History window

Incremental models load from `var('history_start_date')` (dev default **2026-08-01**). For a
full-history backfill:

```bash
dbt build --full-refresh --vars '{history_start_date: "2023-10-01"}'
```

(2B+ CDR rows — use a Medium/Large warehouse for the one-off backfill, then return to X-Small.)

---

## Model counts per layer

| Layer | Folder | Count | Materialisation |
|---|---|---:|---|
| Bronze | `models/bronze` (sources) | **63** source tables (8 client schemas) + landing, S3 external tables and the N04 native seam source | read-only sources |
| Silver | `models/silver/core` | 8 | table |
| | `models/silver/ref` | 5 | table |
| | `models/silver/usage` | 5 | incremental (+1 table) |
| | `models/silver/billing` | 8 | table / incremental |
| | `models/silver/care` | 5 | table |
| | `models/silver/network` | 8 | table / incremental |
| | `models/silver/marketing` | 4 | table / incremental |
| | `models/silver/partner` | 4 | table |
| | `models/silver/intermediate` | 2 | view / table |
| | `models/silver/dq` | 4 | table |
| | `models/silver/landing` | 2 | table |
| | `models/silver/insights` (P01–P12) | 16 | incremental / table |
| | **Silver total** | **71** | |
| Snapshots | `snapshots/` | **6** | SCD2 snapshot |
| Gold | `models/gold/dims` | 18 | table |
| | `models/gold/facts` | 12 | table / incremental |
| | `models/gold/marts` | 8 | table |
| | `models/gold/aggregates` | 29 | table / incremental |
| | `models/gold/analytics` | 31 | table / incremental / 2 dynamic tables |
| | `models/gold/advanced` | 19 | table / incremental / secure view / 1 dynamic table |
| | `models/gold/insights` (P01–P12) | 33 | incremental / table |
| | **Gold total** | **150** | |
| Seeds | `seeds/` | 4 | `sla_policy`, `region_access_map`, `cost_to_serve_rates`, `roaming_iot_discounts` |
| Tests | yml + `tests/singular` | 596 | |
| **Total dbt models** | | **221** (+6 snapshots) | 126 incremental · 90 table · 3 dynamic table · 2 view |

**End-to-end pipelines (bronze → silver → gold final output): 111** — every gold model that no
other model builds on, with its full upstream chain. 17 of them are the outputs of the 12 insight
pipelines below. Operationally all of them run as **one** dbt job (`OPS.T_DBT_BUILD`, 01:10 Manila).

Every model starts with an explicit `{{ config(...) }}` block (materialization, schema, tags,
transient / cluster_by / incremental strategy / unique_key / copy_grants / hooks as applicable).

---

## What each model does

### Silver — core (8)

| Model | Description |
|---|---|
| `slv_customer` | Party master, cleaned; KYC JSON parsed; PII kept only in silver. |
| `slv_account` | Billing accounts with payment method, paperless flag and billing cycle day. |
| `slv_subscription` | Current state of every line; MSISDN normalised; `is_reportable` = not deleted and not a test customer. |
| `slv_subscription_status_history` | Status intervals per line (ACTIVE/SUSPENDED/CHURNED), zero-length intervals removed. |
| `slv_contract` | Contracts with commitment months, auto-renew and termination details. |
| `slv_customer_consent` | Latest consent decision per customer and consent type (DPA compliance). |
| `slv_service_instance` | Fixed-broadband (FTTH/DSL) services joined to their validated service address and location. |
| `slv_sim_inventory` | SIM stock and status; the SIM auth key (Ki) is deliberately not propagated. |

### Silver — ref (5)

| Model | Description |
|---|---|
| `slv_region` | Regions with `UNKNOWN` for missing names and an island-group roll-up. |
| `slv_plan` | Tariff plans with JSON attributes (speed, data cap, promo) and current feature list. |
| `slv_device` | Device catalogue (maker, model, 5G support, list price). |
| `slv_fx_rate` | Daily FX to PHP with valid-from/valid-to intervals for as-of joins. |
| `slv_time_slot` | 48 half-hour slots with peak flag; equi-join key `slot_id`. |

### Silver — usage (5)

| Model | Description |
|---|---|
| `slv_cdr_voice` | Voice CDRs deduped on `call_id`, timezone-normalised, with peak-slot flag (incremental). |
| `slv_cdr_sms` | SMS CDRs deduped on `sms_id`, timezone-normalised (incremental). |
| `slv_cdr_data` | Data sessions (bronze UTC) with duration, bytes and MB (incremental). |
| `slv_xdr_session` | Multi-vendor mediation JSON parsed to one row per session; FAILED records excluded (incremental). |
| `slv_roaming_usage` | TAP roaming usage converted to PHP at the FX rate valid on the usage day. |

### Silver — billing (8)

| Model | Description |
|---|---|
| `slv_invoice` | Invoice headers, deduplicated on `invoice_id`. |
| `slv_invoice_line` | Invoice line items by type (plan fee, add-on, VAS, roaming, tax, discount). |
| `slv_payment` | Payments with local payment date and normalised payment channel. |
| `slv_adjustment` | Credits/adjustments; bronze negative amount exposed as positive `credit_amount_php`. |
| `slv_recharge` | Prepaid top-ups (bronze Manila local time) (incremental). |
| `slv_daily_revenue` | Earned revenue per line, day and stream (incremental, delete+insert by business date). |
| `slv_dunning_event` | Collections reminders and barring actions per invoice. |
| `slv_late_fee_assessment` | Late fees assessed, joined to the fee rule that applied. |

### Silver — care (5)

| Model | Description |
|---|---|
| `slv_ticket` | Care tickets with local open/resolve times and resolution hours. |
| `slv_ticket_status_history` | Ticket status transitions in order. |
| `slv_work_order` | Field work orders with booked appointment slot, arrival-in-slot and first-time-fix flags. |
| `slv_field_technician` | Technicians with region, skills and home base. |
| `slv_nps_survey` | NPS responses with promoter/passive/detractor category and parsed driver answers. |

### Silver — network (8)

| Model | Description |
|---|---|
| `slv_cell_site` | Cell sites with region and island group. |
| `slv_cell_kpi_threshold` | Per-cell KPI thresholds pivoted to columns (drop rate kept on the % scale). |
| `slv_cell_kpi_hourly` | 15-minute PM counters rolled up to cell × local hour; NaN latency removed, drop rate in % (incremental). |
| `slv_alarm_event` | Network alarms with duration and active flag. |
| `slv_outage_incident` | Outage incidents with duration and root cause. |
| `slv_outage_impact` | Lines affected by each outage and their credit eligibility. |
| `slv_network_element` | Network elements (vendor, model, firmware, IP). |
| `slv_ont_health_daily` | FTTH ONT optical telemetry (400M rows) rolled up to ONT × day with a degraded flag (incremental). |

### Silver — marketing (4)

| Model | Description |
|---|---|
| `slv_campaign` | Marketing campaigns with channel, budget and targeting rules. |
| `slv_campaign_response` | Campaign sends per line with open/convert flags; grain = campaign × line × send time (incremental). |
| `slv_churn_score` | Latest churn-model score per line with governed HIGH/MEDIUM/LOW band. |
| `slv_segment_membership` | Marketing segment membership intervals per line. |

### Silver — partner (4)

| Model | Description |
|---|---|
| `slv_partner_brand` | MVNO partner brands with contractual revenue-share %. |
| `slv_partner_rate_card` | Wholesale rate card with validity intervals (duplicate versions removed). |
| `slv_partner_settlement` | Monthly settlements booked per partner. |
| `slv_partner_subscriber_map` | Which lines belong to which MVNO partner. |

### Silver — intermediate (2)

| Model | Description |
|---|---|
| `int_subscriber_profile` | Conformed current profile per line — the single source for region, plan, brand, segment and churn band. |
| `int_subscriber_month_status` | Each line's status at start and end of every month (base, gross adds, churn). |

### Silver — data quality (4)

| Model | Description |
|---|---|
| `dq_ip_address_validation` | Classifies element IPs (IPv4/IPv6/invalid) and checks membership of the regional IP pool (T01). |
| `dq_unit_profile` | Profiles KPI threshold unit values and whether they are convertible (T02). |
| `dq_ip_pool_utilization` | IP pool capacity, CIDR validity and exhaustion risk (T04). |
| `dq_threshold_unit_conversion` | Converts thresholds via the unit-conversion table with round-trip checks (T05). |

### Silver — landing (2)

| Model | Description |
|---|---|
| `slv_landing_device_feed` | Vendor device JSON feed flattened to device × spec and reconciled to the catalogue (P4). |
| `slv_landing_partner_rate` | Loaded partner rate-file rows validated against brand master and rate card (P3). |

### Snapshots (6)

| Snapshot | Description |
|---|---|
| `snap_subscription` | SCD2 of each line's plan, status, home cell, device and brand. |
| `snap_plan_catalog` | SCD2 price book — every tariff change retained. |
| `snap_contract_profile` | SCD2 contract profile: lifecycle stage, plan, price band, payment, brand, region, churn band (H01). |
| `snap_subscriber_device` | SCD2 device / SIM / brand / network-fit profile per line (H02). |
| `snap_cell_config` | SCD2 cell configuration from element config, ports and thresholds (H03). |
| `snap_account_profile` | SCD2 account billing and relationship profile on banded attributes (H04). |

### Gold — dimensions (18)

| Model | Description |
|---|---|
| `dim_date` | Calendar dimension 2015–2027 (Manila business calendar). |
| `dim_region` | Regions with island group. |
| `dim_plan` | Current tariff plans with active base, base share and price rank. |
| `dim_device` | Devices with price tier. |
| `dim_cell` | Cells with region, element vendors and per-cell KPI thresholds. |
| `dim_partner` | MVNO partners with mapped line counts. |
| `dim_customer` | Analyst-safe customer dimension: age band, consent flags, no direct identifiers. |
| `dim_account` | Accounts with total and active line counts. |
| `dim_subscriber` | Current subscriber dimension (SCD1) with masked + hashed MSISDN and tenure band. |
| `dim_subscriber_history` | SCD2 subscriber history from `snap_subscription`. |
| `dim_campaign` | Campaigns with duration. |
| `dim_technician` | Field technicians with region. |
| `dim_contract_history` | SCD2 contract dimension with the attributes that changed in each version (H01). |
| `dim_subscriber_device_history` | SCD2 device/SIM dimension classifying each change (swap, eSIM, 5G upgrade, brand move) (H02). |
| `dim_cell_config_history` | SCD2 cell configuration with latest KPI day (H03). |
| `dim_account_profile_history` | SCD2 account profile flagging value and payment-behaviour changes (H04). |
| `dim_customer_tier` | Customer value tier from revenue, usage, care load and payment behaviour (G3). |
| `dim_partner_rate` | In-force MVNO wholesale rate per brand, service and unit (G4). |

### Gold — facts (12)

| Model | Description |
|---|---|
| `fct_subscriber_usage_daily` | Line × day voice, SMS and data usage (incremental). |
| `fct_revenue_daily` | Line × day earned revenue by stream: plan, recharge, VAS, roaming (incremental). |
| `fct_recharge` | Prepaid top-ups with days since previous recharge (incremental). |
| `fct_invoice` | Invoice grain: billed vs lines vs payments vs credits, outstanding and days to pay. |
| `fct_payment` | Payment grain with days after due date and late flag. |
| `fct_ticket` | Ticket grain with SLA target and attainment, re-opens, field work and NPS. |
| `fct_work_order` | Work orders with arrival time, on-site time and first-time-fix. |
| `fct_cell_kpi_daily` | Cell × day radio KPIs vs thresholds, alarms, breach count and degraded flag (incremental). |
| `fct_outage` | Outage incidents with impacted lines and credit exposure. |
| `fct_campaign_response` | Campaign sends with open/convert outcome (incremental). |
| `fct_subscriber_monthly` | Line × month KPI base: status movement, usage, revenue, recharges, tickets (incremental). |
| `fct_churn_risk` | Blended churn-risk score per line with component breakdown (G2, incremental). |

### Gold — KPI marts (8)

| Model | Description |
|---|---|
| `mart_arpu_monthly` | Governed ARPU = revenue / average billable base, by month, region and contract type. |
| `mart_churn_monthly` | Governed churn: opening base, gross adds, churn, closing base and churn rate. |
| `mart_revenue_assurance_monthly` | Account × month reconciliation of billed vs invoice lines vs earned vs paid vs credits. |
| `mart_ar_aging_monthly` | Receivables aging buckets at each month end. |
| `mart_care_sla_daily` | Tickets opened/resolved, SLA attainment and resolution time per day, category, priority, channel. |
| `mart_network_quality_daily` | Regional network scorecard: degraded/congested cells, KPIs, alarms, outages. |
| `mart_partner_settlement_monthly` | MVNO expected revenue share vs booked settlement and variance. |
| `mart_subscriber_360` | One row per line for care/retention, with next-best-action. |

### Gold — aggregates (29) — ported from E01–E29

| Model | Description |
|---|---|
| `agg_recharge_daily_channel` | Top-ups per day × channel × pack × region with share of day, DoD and 7-day average (E01). |
| `agg_payment_daily_method` | Cash collected per day × channel × method with punctuality vs due date (E02). |
| `agg_invoice_status_monthly` | Invoices per billing month × status × region with collection rate and MoM (E03). |
| `agg_adjustment_reason_monthly` | Credits per month × reason × region with outage context (E04). |
| `agg_dunning_stage_daily` | Dunning actions per day × stage × action × region with cure rate (E05). |
| `agg_late_fee_daily` | Late fees per day × rule × region vs the fee implied by the rule table (E06). |
| `agg_ticket_open_daily` | Tickets opened per day × channel × category × priority × region with SLA attainment (E07). |
| `agg_nps_daily` | NPS per day × region × brand with driver scores and 7-day NPS (E08). |
| `agg_work_order_outcome_daily` | Completed work orders per day × type × outcome × technician region (E09). |
| `agg_campaign_response_daily` | Campaign sends/opens/conversions per day × campaign with audience by segment (E10). |
| `agg_new_accounts_daily` | New accounts per day × payment method with cohort line survival (E11). |
| `agg_sim_inventory_snapshot` | Current SIM stock per type × status × region with stock ageing (E12). |
| `agg_contract_expiry_monthly` | Contracts ending per month × type × auto-renew × churn band, revenue at risk (E13). |
| `agg_consent_snapshot` | Consent coverage per type × channel × age band (E14). |
| `agg_roaming_daily_country` | Roaming per day × country × operator with FX variance of billed PHP (E15). |
| `agg_sms_daily_type` | SMS per day × type × roaming × region with busy hour (E16). |
| `agg_voice_daily_type` | Voice per day × call type × roaming × region: peak share, short calls, p95 duration (E17). |
| `agg_data_daily_rat` | Data per day × RAT × APN × region incl. 5G devices on legacy RATs (E18). |
| `agg_usage_daily_totals` | Usage + revenue per day × region reconciled to legacy `USAGE_DAILY_AGG` (E19). |
| `agg_alarm_severity_daily` | Alarms per day × severity × code × vendor with clearance times (E20). |
| `agg_outage_monthly` | Outages per month × severity × cause × island: MTTR, impact, credit exposure (E21). |
| `agg_element_firmware_snapshot` | Elements per type × vendor × firmware with fault rate and latest-version flag (E22). |
| `agg_port_status_snapshot` | Ports per type × admin/oper status × vendor: capacity and optical health (E23). |
| `agg_device_catalog_summary` | Device catalogue per maker × type with installed base and 5G capability (E24). |
| `agg_plan_active_daily` | Plans on sale per day × contract type × internet service with base on them (E25). |
| `agg_fx_rate_daily` | Daily FX carried forward (ASOF) with DoD move and 30-day volatility (E26). |
| `agg_partner_settlement_monthly` | MVNO settlements per month × brand: effective vs contractual share, USD value (E27). |
| `agg_ticket_status_changes_daily` | Ticket status transitions per day with dwell time and re-opens (E28). |
| `agg_xdr_parse_status_daily` | Mediation feed health per day × source × parse status vs silver coverage (E29). |

### Gold — analytics (31) — ported from M01–M37

| Model | Description |
|---|---|
| `anl_revenue_daily_by_plan` | Daily earned revenue per plan and revenue stream (M01). |
| `anl_region_voice_daily` | Voice traffic per serving region per day (M02). |
| `anl_region_data_daily` | Data traffic per region and RAT per day with 5G coverage-gap share (M03). |
| `anl_account_collection_monthly` | Billed vs paid vs credited per account and month, with dunning and late fees (M05). |
| `anl_technician_monthly` | Field technician productivity per month (M07). |
| `anl_appointment_adherence_monthly` | Share of technicians arriving inside the booked slot (M08). |
| `anl_campaign_conversion_monthly` | Campaign funnel and 30-day revenue uplift of converters (M09). |
| `anl_churn_band_region` | Churn-risk exposure by region and band (M10). |
| `anl_segment_transition_monthly` | Marketing segment migration matrix per month (M11). |
| `anl_device_mix_region` | Device maker and 5G mix of the active base per region (M12). |
| `anl_partner_revenue_monthly` | MVNO partner economics: revenue vs expected and settled share (M13). |
| `anl_roaming_fx_check_monthly` | Roaming re-rated at as-of FX vs billed PHP (M14). |
| `anl_recharge_plan_monthly` | Prepaid recharge behaviour per plan and pack (M15). |
| `anl_kpi_breach_daily` | Hourly KPI threshold breaches per cell and KPI (UNPIVOT) (M16). |
| `anl_outage_credit_monthly` | Outage SLA credit owed vs granted per account (M17). |
| `anl_ont_health_node_daily` | FTTH ONT optical health rolled up to cabinet/node (M18). |
| `anl_ftth_node_utilization_monthly` | Active fibre services vs node capacity at month end (M19). |
| `anl_late_fee_recalc` | Late fees recomputed from the rule table vs what was charged (M20). |
| `anl_xdr_events` | xDR signalling events flattened to one row per event (M21). |
| `anl_nps_answers_monthly` | Average NPS survey driver answers per month (M22). |
| `anl_device_specs` | Device specifications extracted from the SPECS JSON (M23). |
| `anl_kyc_verification` | KYC / SIM-registration quality without personal fields (M24). |
| `anl_campaign_targeting` | Campaign targeting rules exploded per region and sized against the base (M25). |
| `anl_sms_hourly` | SMS traffic per hour (M26). |
| `anl_payment_daily_channel` | Daily cash collection by channel (M29). |
| `anl_ticket_backlog` | Open ticket backlog vs SLA — **dynamic table** (M30). |
| `anl_cell_daily_dt` | Cell daily KPIs — **dynamic table** (M31). |
| `anl_arpu_region_tier_monthly` | Regional ARPU vs national, tiered (M32). |
| `anl_data_quality_daily` | Daily data-quality scorecard of feeds and silver (PASS/WARN/FAIL) (M33). |
| `anl_contract_renewal_candidates` | Contracts ending next month with plan, churn risk and value — retention work-list (M36). |
| `anl_customer_demographics` | Customers and active lines per age band, gender and language (M37). |

### Gold — advanced (19) — ported from H05–H21

| Model | Description |
|---|---|
| `adv_subscriber_journey` | Status journeys (win-back, lost after suspend, recovered, direct churn) via MATCH_RECOGNIZE, enriched with revenue, usage, care, NPS (H05). |
| `adv_data_sessions` | Data records grouped into visits (30-min idle gap) per subscriber and day, with cell congestion context (H06). |
| `adv_roaming_fx_asof` | Monthly roaming re-rated at as-of USD/PHP (ASOF JOIN) and reconciled to billed and booked revenue (H07). |
| `adv_revenue_share_rank` | Secure view: top-10 plans per region and month by revenue with share, ranks and PIVOTed streams (H08). |
| `adv_ftth_hierarchy_rollup` | Daily FTTH tree (recursive CTE OLT → cabinet → splitter → port → line) with health, tickets and revenue (H09). |
| `adv_nearest_technician` | Assigned vs nearest and best-scored technician per work order (ST_DISTANCE, H3) (H10). |
| `adv_outage_geo_impact` | Subscribers inside each outage area (ST_INTERSECTS) vs recorded impact — recall/precision (H11). |
| `adv_monthly_usage_snapshot` | Monthly per-subscriber usage profile: weekday/weekend, streaks, cap use, MoM, revenue, churn (H12). |
| `adv_value_tiers` | RFM value tiers per line and month with tier movement, LTV estimate and recommended action (H13). |
| `adv_cell_anomaly` | Daily cell KPI anomalies vs 29-day baseline (z-score + robust MAD) with likely cause (H14). |
| `adv_billing_close_invoice_totals` | Billing close step 1: invoice totals per close month and account (H15). |
| `adv_billing_close_payment_totals` | Billing close step 2: payments within 45 days against each close month's invoices (H15). |
| `adv_billing_close` | Billing close step 3: billed, paid, credits, late fees, dunning, balance and credit risk per account (H15). |
| `adv_subscription_current_scd1` | Current wide subscription state (SCD1 merge + soft-delete) (H16). |
| `adv_customer_360_json` | One VARIANT customer-360 JSON document per account and month plus flat columns (H17). |
| `adv_revenue_leakage_daily` | Revenue assurance: rated raw CDR/xDR evidence vs booked revenue per line and day, with leakage type (H18). |
| `adv_churn_feature_store` | Churn-model features per line at each month end (trends, care, payments, usage) (H19). |
| `adv_dunning_journey` | Collections journey per invoice via MATCH_RECOGNIZE (cured, barred, written down…) (H20). |
| `adv_cell_hourly_state` | Cell daily state from its 24-hour profile: day-part PIVOTs, degraded episodes, flips — **dynamic table** (H21). |

---

## Insight pipelines (P01–P12)

Twelve cross-domain pipelines built on the same silver / gold layer (no new sources except the
S3 vendor / partner feeds). Folders `models/silver/insights/<pipeline>/` and
`models/gold/insights/<pipeline>/`, tags `insights` + `p01`…`p12`; `dbt build -s tag:p06`
builds one pipeline. Monthly and entity outputs cover the full history (from 2023-10); subscriber ×
day outputs keep a rolling window (`var insights_daily_days`, default 120).

| # | Pipeline | Question it answers | Main inputs |
|---|---|---|---|
| P01 | Customer experience score | How good was each line's network experience today, and who needs saving? | CDR cells, cell KPIs, alarms, outages, network tickets |
| P02 | Churn root-cause attribution | Why did each churned line leave (network / billing / service)? | P01, tickets, outages, bill changes, late payments, suspensions (MATCH_RECOGNIZE) |
| P03 | Cohort retention & lifetime value | How do activation cohorts survive and what is a line worth? | status history, revenue, churn score (Kaplan–Meier, discounted LTV) |
| P04 | Roaming profitability | Which partner / country makes or loses money? | roaming usage, S3 partner-rate feed, FX (ASOF JOIN) |
| P05 | MVNO settlement reconciliation | Does each MVNO settlement match usage × rates? | partner map, usage, rate feed, PARTNER_SETTLEMENT |
| P06 | Collections & dunning | Which dunning stages cure, how fast do customers pay, who is at risk? | invoices, payments, dunning, late fees (MATCH_RECOGNIZE) |
| P07 | Cost-to-serve & line profitability | Which lines cost more to serve than they earn? | tickets, notes, work orders, revenue, seed unit costs |
| P08 | Cell site ROI & capacity priority | Which sites to upgrade first? | CDR traffic per cell, home-cell revenue, PRB load, alarms |
| P09 | FTTH & coverage gaps | Where are coverage / fibre gaps by barangay? | service addresses (H3, ST_CONTAINS), coverage polygons, FTTH topology, ONT health |
| P10 | Device lifecycle & upgrade propensity | Who should get a handset upgrade offer? | device history, S3 vendor device feed, usage, contracts |
| P11 | Campaign incrementality | What revenue did each campaign really add vs a matched control? | campaign responses, untargeted control strata, revenue (difference-in-differences) |
| P12 | Usage & revenue forecasting | What will revenue and data traffic be next 30 days per region? | daily usage / revenue series, `SNOWFLAKE.ML.FORECAST` |

### Models per pipeline

**p01_experience**

| Model | Layer | Materialization | What it does |
|---|---|---|---|
| `dim_p01_subscriber_experience` | gold | table | P01: current 30-day experience profile per line, trend, worst day, and recommended action (SAVE_NOW / FIX_NETWORK / WATCH_DECLINE / OK). |
| `fct_p01_experience_daily` | gold | incremental | P01: customer experience score per line and day (usage-weighted cell quality, outage and network-ticket penalties) with GOOD / FAIR / POOR band. |
| `mart_p01_experience_region_daily` | gold | incremental | P01: experience KPIs per region and day: average and P10 score, % POOR / GOOD lines, outage line-minutes, 7-day trend. |
| `int_p01_cell_day_quality` | silver | incremental | P01: 0-100 quality score per cell and day from PM breaches, critical alarm minutes and regional outages. |
| `int_p01_subscriber_cell_day` | silver | incremental | P01: network usage per line, day and serving cell (voice minutes, data MB, usage weight). |

**p02_churn_root_cause**

| Model | Layer | Materialization | What it does |
|---|---|---|---|
| `fct_p02_churn_attribution` | gold | incremental | P02: one row per churn with recency-weighted cause scores, the MATCH_RECOGNIZE journey (NETWORK_SPIRAL / BILLING_SPIRAL / SERVICE_FAILURE) and the primary root cause. |
| `mart_p02_churn_root_cause_monthly` | gold | incremental | P02: churned lines and lost monthly revenue by month, region, contract type and root cause. |
| `int_p02_churn_events` | silver | incremental | P02: every move of a line into CHURNED with region, plan, tenure and the 3-month average revenue before churn. |
| `int_p02_precursor_events` | silver | incremental | P02: event stream of the 90 days before each churn: tickets, SLA breaches, POOR network days, outages, bill shock, late payments, suspensions. |

**p03_cohort_ltv**

| Model | Layer | Materialization | What it does |
|---|---|---|---|
| `dim_p03_subscriber_ltv` | gold | table | P03: projected remaining lifetime value and LTV decile per active line (segment hazard adjusted by the line's churn score). |
| `fct_p03_cohort_month` | gold | incremental | P03: per cohort, activity month, contract type and region: lines observed, in base, churned, revenue and data. |
| `mart_p03_cohort_retention` | gold | table | P03: Kaplan-Meier survival curve, observed retention and cumulative revenue per acquired line by cohort and contract type. |
| `mart_p03_ltv_segment` | gold | table | P03: ARPU, monthly churn hazard, expected lifetime and discounted LTV per segment (contract x plan category x island group). |
| `int_p03_cohort_members` | silver | table | P03: acquisition cohort (activation month) and LTV segment of every reportable line. |

**p04_roaming_profitability**

| Model | Layer | Materialization | What it does |
|---|---|---|---|
| `fct_roaming_margin_daily` | gold | incremental | P04. |
| `mart_roaming_profitability_monthly` | gold | incremental | P04. |
| `int_roaming_iot_rate_daily` | silver | incremental | P04. |

**p05_mvno_settlement**

| Model | Layer | Materialization | What it does |
|---|---|---|---|
| `fct_mvno_line_monthly` | gold | incremental | P05. |
| `fct_mvno_settlement_recon` | gold | incremental | P05. |
| `mart_mvno_partner_scorecard` | gold | table | P05. |

**p06_collections**

| Model | Layer | Materialization | What it does |
|---|---|---|---|
| `fct_dunning_journey` | gold | incremental | P06. |
| `mart_collections_cure_monthly` | gold | incremental | P06. |
| `mart_collections_risk_segment` | gold | table | P06. |
| `int_invoice_collection` | silver | incremental | P06. |

**p07_cost_to_serve**

| Model | Layer | Materialization | What it does |
|---|---|---|---|
| `fct_line_cost_to_serve_monthly` | gold | incremental | P07. |
| `mart_cost_to_serve_segment_monthly` | gold | incremental | P07. |
| `mart_unprofitable_lines` | gold | table | P07. |
| `int_care_cost_event` | silver | incremental | P07. |

**p08_cell_site_roi**

| Model | Layer | Materialization | What it does |
|---|---|---|---|
| `fct_p08_cell_monthly` | gold | incremental | P08 step 3 - monthly economics and load per cell: home revenue, traffic, PRB p95, congestion, alarms, revenue per GB, month-over-month growth. |
| `mart_p08_site_upgrade_priority` | gold | table | P08 output - capacity upgrade priority per cell: PRB runway (REGR_SLOPE over 90 days), neighbour load within 3 km, revenue at risk, priority score, national / regional ra |
| `int_p08_cell_revenue_monthly` | silver | incremental | P08 step 2 - recognised revenue, lines and ARPU of the subscribers homed on each cell, per month. |
| `int_p08_cell_traffic_daily` | silver | incremental | P08 step 1 - voice, data and SMS traffic carried by each cell per Manila business day (from the three CDR feeds). |

**p09_coverage_gaps**

| Model | Layer | Materialization | What it does |
|---|---|---|---|
| `fct_p09_ont_health_barangay_daily` | gold | incremental | P09 step 3 - FTTH optical health per barangay and day: degraded ONT share, Rx power p05, overheating ONTs, distance-vs-Rx correlation. |
| `mart_p09_coverage_gaps` | gold | table | P09 output 1 - coverage and fixed-network gap per barangay: mobile / 5G coverage share, distance to sites, FTTH optical health, DSL lines near fibre, gap score, gap type  |
| `mart_p09_coverage_h3` | gold | table | P09 output 2 - H3 resolution-7 hexagon grid for coverage maps: lines, coverage share, sites in hex, FTTH health, boundary WKT. |
| `int_p09_location_mobile_coverage` | silver | table | P09 step 2 - mobile coverage per service location: cell polygons containing it (H3_COVERAGE + ST_CONTAINS), nearest cell and nearest 5G cell. |
| `int_p09_service_location` | silver | table | P09 step 1 - fixed-service locations with H3 cells (res 6/7/8), FTTH chain resolved to the serving OLT and distance to the nearest OLT. |

**p10_device_lifecycle**

| Model | Layer | Materialization | What it does |
|---|---|---|---|
| `agg_p10_device_fleet_monthly` | gold | incremental | P10 output 3 - monthly usage, 5G data share and revenue of the handset fleet by region, manufacturer, generation and lifecycle stage (device of record = current device). |
| `mart_p10_device_price_trend` | gold | table | P10 output 1 - lifecycle stage, price trend (REGR_SLOPE per 30 days), price change since launch and installed base per handset model. |
| `mart_p10_upgrade_propensity` | gold | table | P10 output 2 - handset upgrade propensity per active line (logistic score over device age, 5G gap, usage growth, ARPU, contract end, churn band, price drop) with band and |
| `int_p10_device_price_timeline` | silver | table | P10 step 1 - price observations per handset: catalogue launch price plus daily vendor price updates read from the S3 external table. |

**p11_campaign_incrementality**

| Model | Layer | Materialization | What it does |
|---|---|---|---|
| `fct_p11_campaign_uplift` | gold | incremental | P11 output 1 - stratified difference-in-differences revenue uplift per campaign and send month: treated vs control change per line, t-stat, significance, incremental reve |
| `mart_p11_channel_incrementality` | gold | table | P11 output 2 - incremental revenue, ROI and share of significantly positive / negative campaigns per channel and month (complete post windows only). |
| `int_p11_campaign_treated` | silver | incremental | P11 step 1 - treated lines per campaign and send month (first send date, opened, converted) with the matching stratum: region x plan category x churn band x tenure band. |
| `int_p11_control_stratum_daily` | silver | incremental | P11 step 2 - daily revenue of the untargeted control pool per stratum, from 31 days before to 31 days after each send month. |

**p12_forecast**

| Model | Layer | Materialization | What it does |
|---|---|---|---|
| `fct_p12_forecast` | gold | incremental | P12: 30-day Snowflake ML FORECAST per region and metric with 80% prediction interval; every run is kept (forecast_run_date). |
| `fct_p12_forecast_accuracy` | gold | table | P12: past forecasts against actuals: error, absolute percentage error, inside-interval flag. |
| `mart_p12_forecast_region` | gold | table | P12: regional outlook: next 7 / 30 days forecast vs the last 30 actual days, expected change and measured accuracy (MAPE). |
| `int_p12_region_daily_series` | silver | incremental | P12: complete daily revenue (PHP) and data traffic (GB) series per region, the training input for the forecast. |


### Assumptions and data limitations (synthetic data)
* Cost-to-serve unit costs (`seeds/cost_to_serve_rates.csv`) and roaming operator discounts
  (`seeds/roaming_iot_discounts.csv`) are documented assumptions, not client figures.
* Dunned invoices are never paid in the source, so the dunning cure rate is 0%; 12,379 invoices are
  dunned after payment (partly caused by the ingestor remapping dunning events to the next invoice).
* 37% of work orders complete before arrival (negative on-site time) → costed as 0 hours.
* All service addresses sit in a Metro Manila box while cells span the country, so almost every
  barangay is classified `NO_MOBILE_COVERAGE`; only OLTs carry FTTH locations.
* Cell PRB is saturated (~99.9% peak, ~50% average everywhere), so P08 ranks cells on relative load.
* Partner settlements stop at 2026-09; later months show `NOT_SETTLED`.
* P12 retrains the ML forecast on every run (~4 min of the nightly build); P01 / P02 network
  precursors only cover the rolling 120-day window.

---

## Layer rules

| Concern | Rule (applied once, in silver) |
|---|---|
| Time | Every event → `*_utc`, `*_local` (Asia/Manila), `event_date` = Manila business day. Bronze mixes `TIMESTAMP_TZ` (mixed offsets), `LTZ`, NTZ-in-UTC (`CDR_DATA`, `CELL_PERFORMANCE_15MIN`, `ONT_TELEMETRY`) and NTZ-in-Manila (`RECHARGE`, `WORK_ORDER`, status history) — see `macros/tz.sql`. |
| Duplicates | CDRs by id (latest ingest wins); invoice `G0000000101`; 480 rate-card versions; 1,407 zero-length status intervals. |
| Units | `DROP_CALL_RATE` fraction → % (thresholds are in %); `NaN` latency → NULL; bytes → MB. |
| Security | SIM `AUTH_KEY` and churn embeddings not propagated; PII (name, DOB, KYC) stays in silver; gold has masked + hashed MSISDN. |
| Reportability | `is_reportable` = not soft-deleted and not a test customer. Marts filter on it; facts keep everything. |
| KPIs | ONE definition each: ARPU = revenue / avg(BOM, EOM) billable base; churn = BOM-base lines that end the month CHURNED / BOM base. |

## Live estate: how the data keeps arriving (ops/)

The estate behaves like a running telco. Every night one Snowflake task graph runs the whole chain:

```
OPS.T_ROOT  01:10 Asia/Manila (owner DBT_ROLE, warehouse ETL_WH)
 └ T_INGEST       INGEST.SP_INGEST_DAY(yesterday)   source systems: inserts, updates, hard deletes, 1-3% late rows, 0.3% duplicates
    └ T_LOAD_FILES INGEST.SP_LOAD_FILES(yesterday)  Snowpipe (CDR voice/data/SMS, XDR, payments) + exported-vs-loaded reconciliation
                                                 + refresh of the 2 S3 external tables (vendor devices, partner rates)
       ├ T_NATIVE  OPS.SP_RUN_NATIVE(yesterday)     health of NATIVE_PIPE (N01-N40, own root 03:15 Manila); PIPE / PIPE_X retired
       └ T_FRESHNESS  EXECUTE DBT PROJECT ... source freshness   (error after 2 days without a load)
          └ T_DBT_BUILD  EXECUTE DBT PROJECT OPS.TELCO_DBT 'build --target prod'
             └ T_QA       OPS.SP_QA_CHECKS(yesterday)   arrival, Snowpipe reconciliation, bronze=silver, RLS coverage
 T_FINALIZE (finalizer)  OPS.PIPELINE_RUN_LOG: one row per task + graph status
BI_SIM.T_TABLEAU_REFRESH 07/12/17h as SVC_TABLEAU (TELCO_ANALYST_NCR)   BI_SIM.T_POWERBI_REFRESH 08:30/13:30 as SVC_POWERBI (TELCO_ANALYST_VISMIN)
```

| Piece | Where | What it does |
|---|---|---|
| Ingestor | `INGEST.SP_INGEST_DAY` (`ops/sql/02_ingest.sql`) | Replays the same weekday of September 2026 with a time shift into every feed: CDR/xDR/payment **files** to `@INGEST.STG_FEEDS/<feed>/<date>/<run_id>/`, direct DML for BSS/OSS tables, ticket/alarm/outage lifecycles that complete on later days, churn / win-back / suspensions / plan changes, payment allocation and invoice status, CDR voids and payment reversals (hard deletes), month-end bill cycle. One transaction per day; idempotent via `INGEST.FEED_WATERMARK`. |
| Snowpipe | `INGEST.PIPE_*` | Internal stage, so the scheduler triggers `ALTER PIPE ... REFRESH` for the committed run's folder and waits until loaded rows = exported rows. |
| External tables | `INGEST.EXT_DEVICE_FEED`, `INGEST.EXT_PARTNER_RATE` (`ops/sql/11_*`) | Daily vendor device JSON and partner rate CSV files on `s3://devx-telco-native-landing/ingest/` (integration `S3_TELCO_NATIVE`), partitioned by `dt=`. AUTO_REFRESH through the bucket's S3 event notifications, plus an explicit partition refresh in `T_LOAD_FILES`. dbt reads them as source `s3_feeds` in `slv_landing_device_feed` / `slv_landing_partner_rate`. |
| Change detection | `CHANGE_TRACKING = TRUE` on 30 source tables | A table property: source shapes are unchanged. dbt reads `CHANGES(...) AT(last successful run)` and so sees inserts, updates **and deletes**. |
| Native pipelines | `NATIVE_PIPE` / `NATIVE_MART` / `NATIVE_SNAP` (platform team), `OPS.SP_RUN_NATIVE` | **PIPE and PIPE_X are retired (2026-11-21)**: nothing calls them; their outputs are re-implemented in dbt. The new native pipelines N01-N40 run on their own root task (03:15 Manila, after this graph). `T_NATIVE` records their run log and failed tasks in `OPS.NATIVE_RUN_LOG`; QA warns on failures. `OPS.LEGACY_TASK_DECISIONS` records every retired task / schema. PIPE / PIPE_X objects are kept, not dropped. |
| dbt in Snowflake | `OPS.TELCO_DBT` (dbt 1.12.3) | Deployed with `python ops/deploy_dbt_project.py` (bundle + `ops/snowflake_profiles.yml`). |
| Governance | `ops/sql/05_governance.sql`, `seeds/region_access_map.csv`, `macros/governance.sql` | 3 regional analyst roles; map filled from the seed; `RAP_REGION` (region id) / `RAP_REGION_NAME` (region name) re-attached to every regional gold table after each build. Check with `python ops/verify_rls.py`. |
| Lineage | `macros/lineage.sql`, `ops/sql/06_lineage.sql` | `OPS.LINEAGE_EDGES` (declared, republished every run), `OPS.V_ACCESS_LINEAGE` (observed from ACCESS_HISTORY), `OPS.V_LINEAGE_RECONCILE`, and `upstream:` in every object comment. |
| Warehouses / cost | `ETL_WH` (Small), `BI_WH` (X-Small), resource monitor `TELCO_DAILY_RM` | Daily credit cap; notify 75%, suspend 100%. |

### Incremental strategy (macros/cdc.sql)

* **Things that happen are incremental; things that are stay tables; history comes from snapshots.**
  Event feeds and their facts / daily / monthly aggregates are incremental (~ 2/3 of the models);
  dims, masters (<= 600k rows) and "current state" outputs (`mart_subscriber_360`, tiers, profiles) are tables.
* **Silver row models** (tag `cdc`) read bronze `CHANGES` since their watermark (`OPS.DBT_MODEL_WATERMARK`),
  merge on the key, receive deletes as tombstones (`_cdc_action = 'DELETE'`), log them to
  `OPS.DBT_CDC_DELETE_LOG` and purge them. Every run logs the event dates it touched to `OPS.DBT_CHANGED_DATES`.
* **Silver aggregates** (revenue, cell KPI hourly, ONT daily, roaming, late fees) recompute only changed dates.
* **Gold** keeps its window macros; their lower bound is widened to the earliest date changed in the model's
  **own** upstream CDC models since its last success, so a CDR that arrives 5 days late is re-aggregated, and
  a ticket update does not make the CDR marts reprocess. Row facts recompute only changed keys.
* Fallback (first run, watermark older than retention, `--full-refresh`): the original lookback windows.
* Window bounds are resolved to **literal dates** before a model runs, so Snowflake prunes the
  event_date-clustered inputs (a scalar subquery in the WHERE clause would scan full history).
* Generic tests on the 15 largest incremental tables check the **last 7 days** each run
  (`vars: test_recent_window / test_recent_days`); run the full pass weekly:
  `dbt test --vars '{test_recent_days: 0}'`.

### Sources moved out of DBT_V2_RAW (2026-11-22)
`NETWORK.IP_POOL` and `REF.UNIT_CONVERSION` were zero-copy clones of `DBT_V2_RAW.NETWORK_IP_POOL` /
`REF_UNIT_CONVERSION` (moved from the DBT_V2_RAW extract, `ops/sql/13_move_ext_sources.sql`). The dbt sources
`network.ip_pool` and `ref.unit_conversion` feed `dq_ip_address_validation`, `dq_ip_pool_utilization`,
`dq_threshold_unit_conversion`, `dq_unit_profile` and `adv_cell_anomaly`. dbt no longer depends on `DBT_V2_RAW`;
the schema itself was dropped on 2026-11-22 (nothing in dbt or the native pipelines read it).

### Pipeline notifications (Slack) and gold status
* **Slack** (`ops/sql/15_slack_notifications.sql`): `T_ROOT` posts ▶️ STARTED, `T_FINALIZE` posts ✅ COMPLETED
  (tasks, dbt build time) or ❌ FAILED (each failing task + reason). Event-driven: no polling, no hourly
  messages. Integration `TELCO_SLACK` (webhook), secret `OPS.SLACK_TELCO_WEBHOOK` (webhook path kept only in
  Snowflake), notifier `OPS.SP_NOTIFY_PIPELINE` — it can never fail the pipeline. Delivery log:
  `INFORMATION_SCHEMA.NOTIFICATION_HISTORY`.
* **Failure is loud**: after logging, `T_FINALIZE` raises when any task failed, so task history shows FAILED.
* **Is gold current?** `OPS.V_PIPELINE_STATUS` (latest run, failures, last clean run, `IS_GOLD_CURRENT`).
  The BI refresh procedures read it first and tag every dashboard query `bi:<tool>:<dashboard>|data=current|stale`.

### dbt ↔ native seam test
`tests/singular/assert_native_n04_matches_cdr_voice.sql` (warn, failures stored) compares the native
N04 capture (`NATIVE_MART.N04_VOICE_INBOX`, dbt source `native_mart`) with `USAGE.CDR_VOICE` by
`CALL_ID` over the last 3 days of captures: **TZ_SHIFT** (same call, different instant), **FUTURE**
(call time after now) and **MISSING** (CDR ingested > 2 h ago, never captured). It is the check that
would have caught the N04 TIMESTAMP_TZ failure; dbt's own `assert_no_future_events` only sees dbt
tables. Verified: 0 differences on real data, 100% of 2.43M rows flagged when a wall-clock timezone
bug is simulated. Read access: `ops/sql/12_native_seam_test.sql` (DBT_ROLE: SELECT on that table only).

### Operating it

```sql
EXECUTE TASK TELCO_PROD_XL_V2.OPS.T_ROOT;                         -- run the whole chain now
SELECT * FROM OPS.PIPELINE_RUN_LOG ORDER BY logged_at DESC;        -- graph history
SELECT * FROM OPS.INGEST_RUN_LOG  WHERE business_date = CURRENT_DATE - 1;
SELECT * FROM OPS.QA_RESULTS      WHERE business_date = CURRENT_DATE - 1;
SELECT * FROM OPS.V_SOURCE_TO_MEDALLION;                           -- bronze schema -> medallion schema lineage
CALL INGEST.SP_INGEST_DAY('2026-11-21'); CALL INGEST.SP_LOAD_FILES('2026-11-21');   -- one day by hand
```

After changing models: `python ops/deploy_dbt_project.py` (adds a new version of `OPS.TELCO_DBT`).
Do not run a local `dbt build --target etl` between 01:10 and ~02:00 Manila: two builds on the same
tables conflict (aborted transactions); use `EXECUTE TASK OPS.T_ROOT` to run the chain on demand.
Local builds on the pipeline warehouse: `dbt build --target etl` (the `etl` output in `~/.dbt/profiles.yml`).

### Verified state (2026-11-21)

| Check | Result |
|---|---|
| Backfill 2026-10-01 → 11-20 (51 business days) | 97.2M rows by DML + 43.3M rows by Snowpipe (CDR voice/data/SMS, XDR, payments); 1.0M updates; 18,155 hard deletes; 1–3% late CDRs/day; Oct 31 bill run: 454,920 invoices, 1.61M lines, 451k late fees |
| Snowpipe reconciliation | exported rows = loaded rows on every day (the loader fails the run otherwise) |
| Task graph run for 2026-11-20 (inside Snowflake) | 7 tasks, 0 failed: ingest → load → native (15 procs) → freshness → dbt build → QA → finalizer |
| QA (`OPS.QA_RESULTS`) | bronze CDR voice/data distinct keys = silver rows (254,427 / 274,576); revenue ₱17,754,127.80 = ₱17,754,127.80; 0 unmapped regions; 46/46 regional gold tables carry a row access policy |
| CDC | late CDRs re-aggregated back to 11-15; 310 voided CDRs deleted from silver and gold |
| Row-level security (`ops/verify_rls.py`) | PASS: each analyst role sees only its regions in bronze + every regional gold table |
| BI | 12 tagged dashboard queries ran as SVC_TABLEAU / SVC_POWERBI with their analyst roles on BI_WH |
| External tables | 51 daily files each: 2,550 vendor device updates, 6,120 partner rate rows; S3 event notifications reach them |
| Lineage | 720 declared edges from 138 upstream objects (catalog `OBJECT_DEPENDENCIES` had 35) |
| Steady-state build | 8 min on Small; incremental models scan ~72 GB (was 350 GB before literal bounds) |
| Materializations | 126 incremental, 90 table, 3 dynamic tables, 2 views, 6 snapshots |

### Open items
* `NATIVE_PIPE.TASK_N04_STREAM` (platform team) fails every 30 min: `SP_N04_CONSUME_STREAM` inserts `CDR_VOICE.CALL_START` (TIMESTAMP_TZ) into an LTZ column — fix is `CALL_START::TIMESTAMP_LTZ`. QA reports it as WARN.
* PIPE / PIPE_X streams are no longer consumed and will go stale; drop the schemas when the team signs off.
* **Failure alerts:** the finalizer logs every run; email needs a notification integration + verified address.
* `TELCO_COMPUTE_RM` (pre-existing monitor on COMPUTE_WH) is over its quota, so the `dev` target cannot resume COMPUTE_WH until it resets; use `--target etl`.

## Production deployment

1. Add a `prod` output to the `my_project` profile (service user, key-pair auth, role `DBT_ROLE`,
   target schema e.g. `TELCO`) — `generate_schema_name` then writes `SILVER` / `GOLD` with no prefix.
2. Schedule (dbt Cloud job, Airflow, or a Snowflake task running `EXECUTE DBT PROJECT`):
   * hourly : `dbt build -s tag:silver,config.materialized:incremental` (CDR feeds)
   * daily 02:00 Manila : `dbt source freshness; dbt build`
3. Retire procedures in the order of the mapping below; keep them in parallel for one month-end close and
   compare outputs (`tests/singular/assert_monthly_revenue_reconciles_to_silver.sql` is the template).
4. Dynamic tables (`anl_ticket_backlog`, `anl_cell_daily_dt`, `adv_cell_hourly_state`) refresh every hour and
   consume warehouse credits continuously — suspend them in dev (`ALTER DYNAMIC TABLE ... SUSPEND`).

## Procedure → dbt mapping

| Stored procedure | Replaced by |
|---|---|
| `PIPE.SP_GOLD_SUBSCRIBER_MONTHLY_KPI` | `fct_subscriber_monthly` |
| `PIPE.SP_GOLD_REVENUE_RECON`, `PIPE_X.SP_X_H15_*` | `fct_invoice`, `mart_revenue_assurance_monthly`, `adv_billing_close*` |
| `PIPE.SP_GOLD_CHURN_RISK`, `PIPE_X.SP_X_M10/H19` | `fct_churn_risk`, `anl_churn_band_region`, `adv_churn_feature_store` |
| `PIPE.SP_GOLD_CELL_DAILY_QUALITY`, `PIPE_X.SP_X_M16/H14/H21/M31` | `fct_cell_kpi_daily`, `anl_kpi_breach_daily`, `adv_cell_anomaly`, `adv_cell_hourly_state`, `anl_cell_daily_dt` |
| `PIPE.SP_GOLD_SUBSCRIBER_PROFILE`, `PIPE_X.SP_X_H01–H04/H16/H17` | `int_subscriber_profile`, `dim_subscriber`, history snapshots/dims, `adv_subscription_current_scd1`, `adv_customer_360_json` |
| `PIPE.SP_GOLD_PLAN`, `PIPE.SP_GOLD_PARTNER_RATE` | `dim_plan`, `snap_plan_catalog`, `dim_partner_rate` |
| `PIPE.SP_GOLD_CUSTOMER_TIER`, `PIPE_X.SP_X_H13` | `dim_customer_tier`, `adv_value_tiers` |
| `PIPE.SP_SCD2_MERGE` (+ stream/task) | `snap_subscription` |
| `PIPE_X.SP_X_E01–E29` | `agg_*` (29 models) |
| `PIPE_X.SP_X_M01–M37` | `anl_*` (31 models); M04 → `mart_ar_aging_monthly`, M06 → `mart_care_sla_daily` |
| `PIPE_X.SP_X_H05–H21` | `adv_*` (19 models) |
| `PIPE.SP_FEED_NEW_ACTIVITY`, `SP_GROW_HISTORY`, `QA.*`, `LANDING.SP_MAKE_TEST_FILES` | **Not pipelines** — synthetic data generators / migration test traps. Do not migrate. |

Not ported from `telco_v2_core`: T03 + its test (migration-only text-truncation trap), M27/M28 (= silver),
M34/M35 (file export/round-trip mechanics), G1/G5–G8 and P2/P5 (duplicate existing gold models).

## Known data findings (from building this)

* Invoices do not reconcile to `BILLING.DAILY_REVENUE` under any stream definition → ~88% of
  account-months are `EARNED_MISMATCH`. Needs the billing owner; the procedures show the same (72%).
* Bronze feeds end 2026-09-30; `dbt source freshness` will warn until ingestion resumes.
* `REF.GEO_REGION` id 2 has no name → `UNKNOWN`.
* FX history (`REF.CURRENCY_RATE`) starts 2024-01-01 but roaming starts 2023-10 → 964k roaming rows have no USD→PHP rate (warn test).
* `USAGE.USAGE_DAILY_AGG` (legacy pre-aggregate) reports ~6x the data volume of `CDR_DATA` — not trustworthy.
* Campaign targeting JSON references plan codes (P01–P20) that don't exist in the plan catalog.
* Outage credits: ~348k account-months owed PHP 167k in credits with nothing granted.
* Bronze `ADJUSTMENT.AMOUNT` is negative; original H15/H20 logic subtracted it (adding credits to balances) — fixed.
* 9,678 of 9,961 network-element IPv4 addresses fall outside their region's IP pools.
