# -*- coding: utf-8 -*-
"""
US Air Traffic Network Analysis — HW1 Q4
Run after prep_t100_to_network.py has produced:
  - airports.csv
  - us_flights_edges.csv
"""
import os
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt

EDGES_CSV = "us_flights_edges.csv"

edges = pd.read_csv(EDGES_CSV)
G = nx.Graph()
for _, r in edges.iterrows():
    u = str(r["origin"]).strip(); v = str(r["destination"]).strip()
    w = float(r["weight"]) if "weight" in r and pd.notna(r["weight"]) else 1.0
    if u == v: continue
    if G.has_edge(u, v): G[u][v]["weight"] += w
    else: G.add_edge(u, v, weight=w)

# Q4a: connected components
components = list(nx.connected_components(G))
sizes = sorted([len(c) for c in components], reverse=True)
print("Number of components:", len(components))
print("Largest component size:", sizes[0] if sizes else 0)
print("Smallest component size:", sizes[-1] if sizes else 0)
print("Top 10 sizes:", sizes[:10])

# Save giant component with pickle + optional GraphML
giant = max(components, key=len) if components else set()
G_giant = G.subgraph(giant).copy()
import pickle
with open("us_air_giant_component.pkl", "wb") as f:
    pickle.dump(G_giant, f, protocol=pickle.HIGHEST_PROTOCOL)
try:
    nx.write_graphml(G_giant, "us_air_giant_component.graphml")
except Exception as e:
    print("GraphML export skipped:", e)

# Q4b: clustering
clust = nx.clustering(G)
clust_s = pd.Series(clust, name="clustering")
print(clust_s.describe())
plt.figure()
clust_s.hist(bins=50)
plt.xlabel("Clustering coefficient")
plt.ylabel("Frequency")
plt.title("Distribution of clustering coefficients")
plt.tight_layout(); plt.savefig("q4b_clustering_distribution.png")

# Q4c: centralities
deg = dict(G.degree())
bet = nx.betweenness_centrality(G, normalized=True)
close = nx.closeness_centrality(G)
deg_s = pd.Series(deg, name="degree")
bet_s = pd.Series(bet, name="betweenness")
close_s = pd.Series(close, name="closeness")
centrality_df = pd.concat([deg_s, bet_s, close_s], axis=1).fillna(0)
print(centrality_df.describe())

# Histograms
plt.figure(); deg_s.hist(bins=60); plt.xlabel("Degree"); plt.ylabel("Frequency"); plt.title("Degree distribution"); plt.tight_layout(); plt.savefig("q4c_degree_distribution.png")
plt.figure(); bet_s.hist(bins=60); plt.xlabel("Betweenness centrality"); plt.ylabel("Frequency"); plt.title("Betweenness distribution"); plt.tight_layout(); plt.savefig("q4c_betweenness_distribution.png")
plt.figure(); close_s.hist(bins=60); plt.xlabel("Closeness centrality"); plt.ylabel("Frequency"); plt.title("Closeness distribution"); plt.tight_layout(); plt.savefig("q4c_closeness_distribution.png")

def topk(s, k=15):
    return s.sort_values(ascending=False).head(k)

top_deg = topk(deg_s); top_bet = topk(bet_s); top_close = topk(close_s)
top_deg.to_csv("q4c_top15_degree.csv", header=["degree"])
top_bet.to_csv("q4c_top15_betweenness.csv", header=["betweenness"])
top_close.to_csv("q4c_top15_closeness.csv", header=["closeness"])

# Degree log-log
deg_vals = pd.Series(list(deg.values()))
counts = deg_vals.value_counts().sort_index()
plt.figure()
plt.loglog(counts.index, counts.values, marker="o", linestyle="None")
plt.xlabel("Degree"); plt.ylabel("Count"); plt.title("Degree distribution (log-log)")
plt.tight_layout(); plt.savefig("q4c_degree_loglog.png")

print("Saved figures and CSVs to current folder.")
