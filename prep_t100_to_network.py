# -*- coding: utf-8 -*-
"""
Prep T-100 Domestic Market files -> network inputs
Creates:
  - airports.csv (iata,name,city,state,lat,lon)
  - us_flights_edges.csv (origin,destination,weight) aggregated over all months
  - us_flights_edges_by_month.csv (origin,destination,month,weight)
Usage:
  python prep_t100_to_network.py
Place alongside:
  - 288798530_T_T100D_MARKET_ALL_CARRIER.csv
  - 288804893_T_MASTER_CORD.csv
"""
import os
import pandas as pd

RAW_EDGES_CSV    = "288798530_T_T100D_MARKET_ALL_CARRIER.csv"
RAW_AIRPORTS_CSV = "288804893_T_MASTER_CORD.csv"

edges = pd.read_csv(RAW_EDGES_CSV, dtype=str)
air   = pd.read_csv(RAW_AIRPORTS_CSV, dtype=str)
edges.columns = [c.strip().upper() for c in edges.columns]
air.columns   = [c.strip().upper() for c in air.columns]

for col in ["ORIGIN","DEST"]:
    if col not in edges.columns:
        raise ValueError(f"Missing required column '{col}' in {RAW_EDGES_CSV}")
if "MONTH" not in edges.columns:
    edges["MONTH"] = None

for c in ["PASSENGERS","DEPARTURES_PERFORMED","FREIGHT","MAIL","SEATS"]:
    if c in edges.columns:
        edges[c] = pd.to_numeric(edges[c], errors="coerce").fillna(0.0)

def clean_iata(s):
    if not isinstance(s, str):
        return None
    s = s.strip().upper()
    if len(s) != 3 or s in {"ZZZ","UNK","","N/A"}:
        return None
    return s

edges["ORIGIN"] = edges["ORIGIN"].map(clean_iata)
edges["DEST"]   = edges["DEST"].map(clean_iata)
edges = edges.dropna(subset=["ORIGIN","DEST"])

if "AIRPORT" not in air.columns:
    raise ValueError(f"Missing 'AIRPORT' in {RAW_AIRPORTS_CSV}")
air["IATA"] = air["AIRPORT"].map(clean_iata)
air = air.dropna(subset=["IATA"]).drop_duplicates(subset=["IATA"], keep="first")

if "AIRPORT_COUNTRY_CODE_ISO" in air.columns:
    air_us = air[air["AIRPORT_COUNTRY_CODE_ISO"].fillna("").str.upper().eq("US")].copy()
else:
    air_us = air.copy()

def to_float_series(df, col):
    if col in df.columns:
        return pd.to_numeric(df[col], errors="coerce")
    return pd.Series([None]*len(df))

lat = to_float_series(air_us, "LATITUDE")
lon = to_float_series(air_us, "LONGITUDE")

def dms_to_decimal(deg, minu, sec, hemi):
    try:
        deg = float(deg); minu = float(minu); sec = float(sec)
        sign = -1.0 if str(hemi).strip().upper() in {"S","W"} else 1.0
        return sign * (abs(deg) + minu/60.0 + sec/3600.0)
    except Exception:
        return None

if lat.isna().all() and {"LAT_DEGREES","LAT_MINUTES","LAT_SECONDS","LAT_HEMISPHERE"}.issubset(air_us.columns):
    lat = air_us.apply(lambda r: dms_to_decimal(r["LAT_DEGREES"], r["LAT_MINUTES"], r["LAT_SECONDS"], r["LAT_HEMISPHERE"]), axis=1)
if lon.isna().all() and {"LON_DEGREES","LON_MINUTES","LON_SECONDS","LON_HEMISPHERE"}.issubset(air_us.columns):
    lon = air_us.apply(lambda r: dms_to_decimal(r["LON_DEGREES"], r["LON_MINUTES"], r["LON_SECONDS"], r["LON_HEMISPHERE"]), axis=1)

airports_out = pd.DataFrame({
    "iata": air_us["IATA"],
    "name": air_us.get("DISPLAY_AIRPORT_NAME", pd.Series([""]*len(air_us))),
    "city": air_us.get("DISPLAY_AIRPORT_CITY_NAME_FULL", pd.Series([""]*len(air_us))),
    "state": air_us.get("AIRPORT_STATE_CODE", pd.Series([""]*len(air_us))),
    "lat": lat,
    "lon": lon
})
used_iata = pd.Index(edges["ORIGIN"]).union(pd.Index(edges["DEST"]))
airports_out = airports_out[airports_out["iata"].isin(used_iata)].drop_duplicates(subset=["iata"], keep="first")

weight_col = None
for c in ["PASSENGERS","DEPARTURES_PERFORMED","FREIGHT","MAIL","SEATS"]:
    if c in edges.columns:
        weight_col = c; break

group_cols = ["ORIGIN","DEST"]
monthly = edges[group_cols + (["MONTH"] if "MONTH" in edges.columns else [])].copy()
if weight_col:
    monthly["weight"] = edges[weight_col].astype(float)
    agg = monthly.groupby(group_cols + ["MONTH"], dropna=False)["weight"].sum().reset_index()
else:
    monthly["weight"] = 1.0
    agg = monthly.groupby(group_cols + ["MONTH"], dropna=False)["weight"].sum().reset_index()

agg.rename(columns={"ORIGIN":"origin","DEST":"destination","MONTH":"month"}, inplace=True)
edges_out = agg.groupby(["origin","destination"], as_index=False)["weight"].sum()

valid = set(airports_out["iata"])
edges_out = edges_out[edges_out["origin"].isin(valid) & edges_out["destination"].isin(valid)]

airports_out.to_csv("airports.csv", index=False)
edges_out.to_csv("us_flights_edges.csv", index=False)
agg.to_csv("us_flights_edges_by_month.csv", index=False)

print("airports.csv:", len(airports_out), "airports")
print("us_flights_edges.csv:", len(edges_out), "edges")
print("us_flights_edges_by_month.csv:", len(agg), "rows")
