import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from io import StringIO
from scipy.stats import binomtest, ttest_1samp, chi2_contingency

# load and parse html
with open("trimet_stopevents_2022-12-07.html", "r", encoding="utf-8") as f:
    raw_html = f.read()
bs = BeautifulSoup(raw_html, "html.parser")

# find headings for each trip
headings = bs.find_all("h2", string=lambda t: t and t.strip().startswith("Stop events for PDX_TRIP"))

frames = []
base_date = datetime.strptime("2022-12-07", "%Y-%m-%d")

for hdr in headings:
    text = hdr.get_text(strip=True)
    parts = text.split()
    trip = parts[-1]
    tbl = hdr.find_next_sibling("table")
    if tbl is None:
        tbl = hdr.find_next("table")
    if tbl is None:
        raise RuntimeError(f"no table after heading '{text}'")

    df_temp = pd.read_html(StringIO(str(tbl)))[0]
    df_temp["tstamp"] = df_temp["arrive_time"].apply(
        lambda s: base_date + timedelta(seconds=int(s))
    )
    sub_df = df_temp[["vehicle_number", "tstamp", "location_id", "ons", "offs"]].copy()
    sub_df.insert(0, "trip_id", trip)
    frames.append(sub_df)

# combine all trips
all_stops = pd.concat(frames, ignore_index=True)

# summary of loaded data
print(f"loaded stop events: {len(all_stops):,}")
if len(all_stops) == 93912:
    print("loaded expected 93,912 records")
else:
    print(f"expected 93,912 but got {len(all_stops):,}")

all_stops = all_stops[["trip_id", "vehicle_number", "tstamp", "location_id", "ons", "offs"]]

def analyze_stops(df):
    print("\n--- basic stop info ---")
    unique_buses = df["vehicle_number"].nunique()
    print(f"unique vehicles: {unique_buses:,}")
    unique_locs = df["location_id"].nunique()
    print(f"unique stop locations: {unique_locs:,}")
    start_time = df["tstamp"].min()
    end_time = df["tstamp"].max()
    print(f"time window: {start_time} to {end_time}")
    boarding = df[df["ons"] >= 1]
    count_board = len(boarding)
    print(f"stops with boarding: {count_board:,}")
    total = len(df)
    pct = (count_board / total) * 100
    print(f"boarding rate: {pct:.2f}%")
    print("-" * 30)

def analyze_cases(df):
    print("\n--- specific location & vehicle ---")
    loc_data = df[df["location_id"] == 6913]
    print(f"location 6913 stops: {len(loc_data):,}")
    buses_at_loc = loc_data["vehicle_number"].nunique()
    print(f"buses at 6913: {buses_at_loc:,}")
    boarding_loc = loc_data[loc_data["ons"] >= 1]
    pct_loc = (len(boarding_loc) / len(loc_data)) * 100 if len(loc_data) else 0
    print(f"boarding % at 6913: {pct_loc:.2f}%")
    veh_data = df[df["vehicle_number"] == 4062]
    print(f"vehicle 4062 stops: {len(veh_data):,}")
    boarded = veh_data["ons"].sum()
    deboarded = veh_data["offs"].sum()
    print(f"4062 onboarded total: {boarded:,}")
    print(f"4062 offboarded total: {deboarded:,}")
    boarding_veh = veh_data[veh_data["ons"] >= 1]
    pct_veh = (len(boarding_veh) / len(veh_data)) * 100 if len(veh_data) else 0
    print(f"boarding % for 4062: {pct_veh:.2f}%")
    print("-" * 30)

def get_biased(df, alpha=0.05):
    print("\n--- boarding bias test ---")
    total = len(df)
    boarded_all = len(df[df["ons"] >= 1])
    system_rate = boarded_all / total
    print(f"system boarding rate: {system_rate:.4f} ({system_rate*100:.2f}%)")
    buses = df["vehicle_number"].unique()
    biased = []
    print(f"\ntesting {len(buses)} vehicles")
    print("bus_id | stops | boarded_stops | rate | p-val")
    print("-" * 60)
    for b in buses:
        data_b = df[df["vehicle_number"] == b]
        n = len(data_b)
        boarded_b = len(data_b[data_b["ons"] >= 1])
        rate_b = boarded_b / n if n else 0
        res = binomtest(boarded_b, n, system_rate)
        p = res.pvalue
        mark = "*" if p < alpha else ""
        print(f"{b:6} | {n:5} | {boarded_b:13} | {rate_b:5.4f} | {p:.6f} {mark}")
        if p < alpha:
            biased.append({"bus": b, "p": p})
    if biased:
        print("\nbuses biased (p < {:.2f}):".format(alpha))
        for item in biased:
            print(f"bus {item['bus']}: p={item['p']:.6f}")
    else:
        print("\nno biased buses found")
    return biased

def get_gps_bias(df, alpha=0.005):
    print("\n--- gps relpos analysis ---")
    vals = df["RELPOS"].dropna().values
    mean_all = vals.mean()
    std_all = vals.std()
    print(f"overall mean: {mean_all:.6f}, std: {std_all:.6f}, count: {len(vals):,}")
    buses = df["vehicle_number"].unique()
    biased = []
    print(f"\ntesting gps for {len(buses)} vehicles")
    print("bus_id | count | mean | std | p-val")
    print("-" * 60)
    for b in buses:
        vdata = df[df["vehicle_number"] == b]
        rels = vdata["RELPOS"].dropna()
        if len(rels) < 10:
            continue
        m = rels.mean()
        s = rels.std()
        t_stat, p = ttest_1samp(rels, mean_all)
        mark = "*" if p < alpha else ""
        print(f"{b:6} | {len(rels):5} | {m:7.6f} | {s:7.6f} | {p:.6f} {mark}")
        if p < alpha:
            direction = "RIGHT" if m > 0 else "LEFT"
            biased.append({"bus": b, "p": p, "mean": m, "dir": direction})
    if biased:
        print("\nbuses with gps bias (p < {:.3f}):".format(alpha))
        for item in biased:
            print(f"bus {item['bus']}: p={item['p']:.6f}, mean={item['mean']:.6f} ({item['dir']})")
    else:
        print("\nno gps-biased buses found")
    return biased

def get_offs_ons_bias(df, alpha=0.05):
    print("\n--- offs/ons chi-square ---")
    total_offs = df["offs"].sum()
    total_ons = df["ons"].sum()
    tot_pass = total_offs + total_ons
    prop_offs = total_offs / tot_pass if tot_pass else 0
    prop_ons = total_ons / tot_pass if tot_pass else 0
    print(f"system offs: {total_offs:,}, ons: {total_ons:,}")
    print(f"offs %: {prop_offs:.4f}, ons %: {prop_ons:.4f}")
    buses = df["vehicle_number"].unique()
    biased = []
    print(f"\ntesting {len(buses)} vehicles")
    print("bus_id | offs | ons | offs% | ons% | p-val")
    print("-" * 65)
    for b in buses:
        vdata = df[df["vehicle_number"] == b]
        v_offs = int(vdata["offs"].sum())
        v_ons = int(vdata["ons"].sum())
        v_tot = v_offs + v_ons
        if v_tot == 0:
            continue
        off_p = v_offs / v_tot
        on_p = v_ons / v_tot
        table = [
            [v_offs, v_ons],
            [total_offs - v_offs, total_ons - v_ons]
        ]
        chi2, p, dof, exp = chi2_contingency(table)
        if (exp < 5).any():
            continue
        mark = "*" if p < alpha else ""
        print(f"{b:6} | {v_offs:4} | {v_ons:3} | {off_p*100:5.2f}% | {on_p*100:5.2f}% | {p:.6f} {mark}")
        if p < alpha:
            bias = "more offs" if off_p > prop_offs else "more ons"
            biased.append({"bus": b, "p": p, "bias": bias})
    if biased:
        print("\nbuses with offs/ons bias (p < {:.2f}):".format(alpha))
        for item in biased:
            print(f"bus {item['bus']}: p={item['p']:.6f} ({item['bias']})")
    else:
        print("\nno offs/ons biased buses found")
    print("-" * 30)
    return biased

# run analyses
analyze_stops(all_stops)
analyze_cases(all_stops)
biased_buses = get_biased(all_stops)

# load gps data
print("\nloading gps data")
gps_data = pd.read_csv("trimet_relpos_2022-12-07.csv")
print(f"gps records: {len(gps_data):,}")
gps_data = gps_data.rename(columns={"VEHICLE_NUMBER": "vehicle_number"})
biased_gps = get_gps_bias(gps_data)

# test offs/ons bias
biased_offs_ons = get_offs_ons_bias(all_stops)
