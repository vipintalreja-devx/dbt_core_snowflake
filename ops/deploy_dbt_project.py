"""Deploy this dbt project as a Snowflake DBT PROJECT object (TELCO_PROD_XL_V2.OPS.TELCO_DBT).

  1. builds a bundle: project files + dbt_packages (vendored, no external access needed)
     + ops/snowflake_profiles.yml as profiles.yml
  2. uploads it to @OPS.DBT_PROJECT_STAGE/telco/<version>/
  3. CREATE DBT PROJECT ... (first time) or ALTER DBT PROJECT ... ADD VERSION (afterwards)

Run from the project root after `dbt deps`:  python ops/deploy_dbt_project.py
"""
import os, shutil, sys, tempfile, time, warnings
warnings.filterwarnings("ignore")
import snowflake.connector
from cryptography.hazmat.primitives import serialization

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INCLUDE = ["dbt_project.yml", "packages.yml", "package-lock.yml", "models", "macros", "seeds",
           "snapshots", "tests", "analyses", "dbt_packages"]
DB, SCHEMA, PROJECT, STAGE = "TELCO_PROD_XL_V2", "OPS", "TELCO_DBT", "DBT_PROJECT_STAGE"
DBT_VERSION = "1.12.3"

def connect():
    key = serialization.load_pem_private_key(open(os.path.expanduser("~/.ssh/snowflake/rsa_key.p8"), "rb").read(), password=None)
    pk = key.private_bytes(serialization.Encoding.DER, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
    return snowflake.connector.connect(account=os.environ.get("SF_ACCOUNT", "BQYBJZW-XH07906"),
                                       user=os.environ.get("SF_USER", "GRAIYANI"), private_key=pk,
                                       role="DBT_ROLE", warehouse="ETL_WH", database=DB, schema=SCHEMA)

def main():
    if not os.path.isdir(os.path.join(ROOT, "dbt_packages")):
        sys.exit("run `dbt deps` first (dbt_packages is vendored into the bundle)")
    version = time.strftime("v%Y%m%d_%H%M%S")
    bundle = tempfile.mkdtemp(prefix="telco_dbt_")
    for item in INCLUDE:
        src = os.path.join(ROOT, item)
        if os.path.isdir(src):
            shutil.copytree(src, os.path.join(bundle, item), ignore=shutil.ignore_patterns("*.pyc", "__pycache__", ".DS_Store"))
        elif os.path.exists(src):
            shutil.copy2(src, bundle)
    shutil.copy2(os.path.join(ROOT, "ops", "snowflake_profiles.yml"), os.path.join(bundle, "profiles.yml"))

    con = connect(); cur = con.cursor()
    cur.execute(f"create stage if not exists {STAGE} comment = 'dbt project bundles for {PROJECT}'")
    prefix = f"@{STAGE}/telco/{version}"
    n = 0
    for dirpath, _, files in os.walk(bundle):
        rel = os.path.relpath(dirpath, bundle)
        target = prefix if rel == "." else f"{prefix}/{rel}"
        for f in files:
            path = os.path.join(dirpath, f).replace("'", "\\'")
            cur.execute(f"put 'file://{path}' '{target}' auto_compress = false overwrite = true parallel = 8")
            n += 1
    cur.execute(f"show dbt projects like '{PROJECT}' in schema {DB}.{SCHEMA}")
    exists = bool(cur.fetchall())
    if exists:
        cur.execute(f"alter dbt project {PROJECT} add version from '{prefix}'")
        cur.execute(f"alter dbt project {PROJECT} set dbt_version = '{DBT_VERSION}' default_target = 'prod'")
    else:
        cur.execute(f"""create dbt project {PROJECT} from '{prefix}' dbt_version = '{DBT_VERSION}' default_target = 'prod'
                        comment = 'Telco medallion (bronze -> silver -> gold); executed by OPS.T_DBT_BUILD'""")
    print(f"deployed {n} files as {DB}.{SCHEMA}.{PROJECT} ({'new version' if exists else 'created'}) from {prefix}")
    shutil.rmtree(bundle, ignore_errors=True)

if __name__ == "__main__":
    main()
