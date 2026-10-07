#!/usr/bin/env python3
"""Compare an OperatorResult TSON written by the Rust operator with the R operator's golden CSVs.
usage: compare_result_tson.py result.tson <golden_dir>   (needs pytson: pip install git+https://github.com/tercen/pytson)
"""
import sys, csv, math
import pytson
tson_path, gold = sys.argv[1], sys.argv[2]
import io; res = pytson.decodeTSON(io.BytesIO(open(tson_path, "rb").read()))
tables = {t["properties"]["name"]: t for t in res["tables"]}
def col(t, name):
    c = next(c for c in t["columns"] if c["name"] == name); v = c["values"]
    return list(v) if not hasattr(v, "tolist") else v.tolist()
def read_csv(p):
    with open(p, newline="") as f:
        r = csv.DictReader(f); rows = list(r); return r.fieldnames, rows
def num_eq(a, b, tol=1e-6):
    try: b = float(b)
    except ValueError: return False
    if math.isnan(a) and (math.isnan(b)): return True
    return abs(a - b) <= tol * max(1.0, abs(b))
ok = True
# Measurements vs test_1_out_1.csv (event_id, channel_id, value) — golden order is arrange(event_id, channel_id)
names, rows = read_csv(f"{gold}/test_1_out_1.csv")
m = tables["Measurements"]; e, c, v = col(m, "event_id"), col(m, "channel_id"), col(m, "value")
if len(rows) != len(e): print(f"Measurements rows: rust {len(e)} vs golden {len(rows)}"); ok = False
bad = sum(1 for i, r in enumerate(rows[:len(e)]) if not (num_eq(e[i], r["event_id"]) and num_eq(c[i], r["channel_id"]) and num_eq(v[i], r["value"])))
print(f"Measurements: {len(e)} rows, {bad} mismatches"); ok &= bad == 0
# Variables vs test_1_out_2.csv
names, rows = read_csv(f"{gold}/test_1_out_2.csv"); t = tables["Variables"]
for n in names:
    rc = col(t, n)
    b = sum(1 for i, r in enumerate(rows) if not (num_eq(rc[i], r[n]) if n == "channel_id" else rc[i] == r[n]))
    print(f"Variables.{n}: {len(rc)} rows, {b} mismatches"); ok &= b == 0 and len(rc) == len(rows)
# Observations vs test_1_out_3.csv
names, rows = read_csv(f"{gold}/test_1_out_3.csv"); t = tables["Observations"]
tn = [c["name"] for c in t["columns"]]
print(f"Observations columns rust={tn} golden={names}"); ok &= tn == names
for n in names:
    rc = col(t, n)
    b = sum(1 for i, r in enumerate(rows) if not (rc[i] == r[n] if n == "filename" else num_eq(rc[i], r[n])))
    print(f"Observations.{n}: {b} mismatches"); ok &= b == 0
s = tables["Summary"]; print("Summary:", col(s, "filename"), col(s, "mimetype"))
print("PARITY", "OK" if ok else "FAILED"); sys.exit(0 if ok else 1)
