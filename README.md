# Telco dbt — Bronze → Silver → Gold on `TELCO_PROD_XL_V2`

Medallion pipeline for the Philippine telco dataset in `TELCO_PROD_XL_V2`, replacing the
stored-procedure pipelines in `PIPE` / `PIPE_X`.

```
BRONZE    TELCO_PROD_XL_V2.{CORE,BILLING,USAGE,CARE,NETWORK,MARKETING,PARTNER,REF,LANDING}
   │        61 source tables (dbt sources, read-only) ▸ freshness checks on CDR / xDR feeds
SILVER    <db>.<schema>_SILVER    55 models — cleaned, deduped, typed, timezone-normalised, one rule-set
   │        ▸ incremental CDR / revenue / PM / ONT feeds (merge or delete+insert, 3-day lookback)
   │        ▸ int_subscriber_profile = the ONE place region / plan / brand / churn band are resolved
SNAPSHOTS <db>.SNAPSHOTS          6 SCD2 snapshots
GOLD      <db>.<schema>_GOLD      117 models — 18 dims · 12 facts · 8 KPI marts · 29 aggregates · 31 analytics · 19 advanced
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

The project reads bronze from `TELCO_PROD_XL_V2` and writes silver/gold into the database configured
in your profile (default `ANALYTICS`). Replace `<YOUR_USER>` below:

```sql
USE ROLE ACCOUNTADMIN;
CREATE ROLE IF NOT EXISTS DBT_ROLE;
GRANT ROLE DBT_ROLE TO USER <YOUR_USER>;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE DBT_ROLE;

-- write target
CREATE DATABASE IF NOT EXISTS ANALYTICS;
GRANT USAGE, CREATE SCHEMA ON DATABASE ANALYTICS TO ROLE DBT_ROLE;

-- read bronze
GRANT USAGE ON DATABASE TELCO_PROD_XL_V2 TO ROLE DBT_ROLE;
GRANT USAGE ON ALL SCHEMAS IN DATABASE TELCO_PROD_XL_V2 TO ROLE DBT_ROLE;
GRANT SELECT ON ALL TABLES IN DATABASE TELCO_PROD_XL_V2 TO ROLE DBT_ROLE;
GRANT SELECT ON ALL VIEWS  IN DATABASE TELCO_PROD_XL_V2 TO ROLE DBT_ROLE;
GRANT SELECT ON FUTURE TABLES IN DATABASE TELCO_PROD_XL_V2 TO ROLE DBT_ROLE;

-- dynamic tables (anl_ticket_backlog, anl_cell_daily_dt, adv_cell_hourly_state)
GRANT CREATE DYNAMIC TABLE ON FUTURE SCHEMAS IN DATABASE ANALYTICS TO ROLE DBT_ROLE;
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
      database: ANALYTICS                    # where silver / gold are written
      schema: DBT_<YOUR_NAME>                # your personal dev prefix -> DBT_<YOU>_SILVER, DBT_<YOU>_GOLD
      threads: 4
```

### 6. Verify the connection

```bash
dbt debug          # must end with "All checks passed!"
```

### 7. Install packages and build

```bash
dbt deps           # installs dbt_utils
dbt seed           # loads seeds/sla_policy.csv
dbt snapshot       # SCD2 snapshots (run BEFORE build - history dims read them)
dbt build          # all models + tests
```

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
| dynamic table refresh fails | the owner role needs the grants itself (dynamic tables refresh as their owner) |
| build slow / queued | warehouse is shared; build one layer at a time: `dbt build -s tag:silver`, then `tag:gold` |

---

## Run (day to day)

```bash
source .venv/bin/activate
dbt snapshot                   # daily, BEFORE build (SCD2 history)
dbt build                      # incremental: only new days are processed
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
| Bronze | `models/bronze` (sources) | **61** source tables | read-only sources |
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
| | **Silver total** | **55** | |
| Snapshots | `snapshots/` | **6** | SCD2 snapshot |
| Gold | `models/gold/dims` | 18 | table |
| | `models/gold/facts` | 12 | table / incremental |
| | `models/gold/marts` | 8 | table |
| | `models/gold/aggregates` | 29 | table / incremental |
| | `models/gold/analytics` | 31 | table / incremental / 2 dynamic tables |
| | `models/gold/advanced` | 19 | table / incremental / secure view / 1 dynamic table |
| | **Gold total** | **117** | |
| Seeds | `seeds/` | 1 | `sla_policy` |
| Tests | yml + `tests/singular` | 470 | |
| **Total dbt models** | | **172** (+6 snapshots) | |

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

## Layer rules

| Concern | Rule (applied once, in silver) |
|---|---|
| Time | Every event → `*_utc`, `*_local` (Asia/Manila), `event_date` = Manila business day. Bronze mixes `TIMESTAMP_TZ` (mixed offsets), `LTZ`, NTZ-in-UTC (`CDR_DATA`, `CELL_PERFORMANCE_15MIN`, `ONT_TELEMETRY`) and NTZ-in-Manila (`RECHARGE`, `WORK_ORDER`, status history) — see `macros/tz.sql`. |
| Duplicates | CDRs by id (latest ingest wins); invoice `G0000000101`; 480 rate-card versions; 1,407 zero-length status intervals. |
| Units | `DROP_CALL_RATE` fraction → % (thresholds are in %); `NaN` latency → NULL; bytes → MB. |
| Security | SIM `AUTH_KEY` and churn embeddings not propagated; PII (name, DOB, KYC) stays in silver; gold has masked + hashed MSISDN. |
| Reportability | `is_reportable` = not soft-deleted and not a test customer. Marts filter on it; facts keep everything. |
| KPIs | ONE definition each: ARPU = revenue / avg(BOM, EOM) billable base; churn = BOM-base lines that end the month CHURNED / BOM base. |

## Production deployment

1. Add a `prod` output to the `my_project` profile (service user, key-pair auth, role `DBT_ROLE`,
   target schema e.g. `TELCO`) — `generate_schema_name` then writes `SILVER` / `GOLD` with no prefix.
2. Schedule (dbt Cloud job, Airflow, or a Snowflake task running `EXECUTE DBT PROJECT`):
   * hourly : `dbt build -s tag:silver,config.materialized:incremental` (CDR feeds)
   * daily 02:00 Manila : `dbt source freshness; dbt snapshot; dbt build`
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
* `USAGE.USAGE_DAILY_AGG` (legacy pre-aggregate) reports ~6x the data volume of `CDR_DATA` — not trustworthy.
* Campaign targeting JSON references plan codes (P01–P20) that don't exist in the plan catalog.
* Outage credits: ~348k account-months owed PHP 167k in credits with nothing granted.
* Bronze `ADJUSTMENT.AMOUNT` is negative; original H15/H20 logic subtracted it (adding credits to balances) — fixed.
* 9,678 of 9,961 network-element IPv4 addresses fall outside their region's IP pools.
