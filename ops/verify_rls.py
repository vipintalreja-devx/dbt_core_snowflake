"""Row-level security check: run each analyst role WITHOUT secondary roles and confirm it
only sees its mapped regions (bronze NETWORK.OUTAGE_INCIDENT + every gold table with REGION_ID).
Usage: python ops/verify_rls.py   (uses the same key-pair connection as the dbt profile)"""
import os, sys, warnings
warnings.filterwarnings("ignore")
import snowflake.connector
from cryptography.hazmat.primitives import serialization

key = serialization.load_pem_private_key(open(os.path.expanduser("~/.ssh/snowflake/rsa_key.p8"), "rb").read(), password=None)
pk = key.private_bytes(serialization.Encoding.DER, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
con = snowflake.connector.connect(account=os.environ.get("SF_ACCOUNT", "BQYBJZW-XH07906"), user=os.environ.get("SF_USER", "GRAIYANI"),
                                  private_key=pk, database="TELCO_PROD_XL_V2", warehouse="BI_WH")
cur = con.cursor()
q = lambda s: (cur.execute(s), cur.fetchall())[1]
q("use role DBT_ROLE")
expected = {}
for role, rid in q("""select m.role_name, g.region_id from GOV.REGION_ACCESS_MAP m join REF.GEO_REGION g
                      on coalesce(nullif(trim(g.region_name), ''), 'REGION_' || g.region_id) = m.region_name"""):
    expected.setdefault(role, set()).add(rid)
names = {}
for rid, nm in q("select region_id, coalesce(nullif(trim(region_name), ''), 'REGION_' || region_id) from REF.GEO_REGION"):
    names[rid] = nm
gold_cols = q("""select c.table_name, listagg(lower(c.column_name), ',') within group (order by c.column_name)
                   from information_schema.columns c join information_schema.tables t
                     on t.table_schema = c.table_schema and t.table_name = c.table_name
                  where c.table_schema = 'DBT_TELCO_GOLD' and t.table_type = 'BASE TABLE'
                    and c.column_name in ('REGION_ID', 'HOME_REGION_ID', 'REGION_NAME')
                  group by 1 order by 1""")
targets = [("NETWORK.OUTAGE_INCIDENT", "region_id")]
for t, cols in gold_cols:
    cols = cols.split(',')
    col = 'region_id' if 'region_id' in cols else ('home_region_id' if 'home_region_id' in cols else 'region_name')
    targets.append((f"DBT_TELCO_GOLD.{t}", col))
fail = 0
for role in sorted(expected):
    allowed_ids = expected[role]
    allowed_names = {names[i].upper() for i in allowed_ids} | ({'UNKNOWN'} if 2 in allowed_ids else set())
    q(f"use role {role}"); q("use secondary roles none")
    for t, col in targets:
        try:
            seen = {r[0] for r in q(f"select distinct {col} from {t} where {col} is not null")}
        except Exception as e:
            print(f"ERROR {role:22s} {t}: {str(e).splitlines()[0][:90]}"); fail += 1; continue
        if col == 'region_name':
            leak = {v for v in seen if str(v).upper() not in allowed_names}
        else:
            leak = seen - allowed_ids
        if leak: fail += 1
        status = "LEAK" if leak else ("OK  " if seen else "EMPTY")
        print(f"{status} {role:22s} {t:52s} [{col}] sees {len(seen)}" + (f"  LEAKED {sorted(map(str, leak))[:6]}" if leak else ""))
print("RESULT:", "PASS" if fail == 0 else f"FAIL ({fail})")
sys.exit(1 if fail else 0)
