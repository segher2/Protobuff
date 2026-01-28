from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

# =========================
# Configuration
# =========================

CSV_PATH = Path("bench_out/size/size_summary.csv")
OUT_DIR = Path("bench_out/size/figures_telling")
OUT_DIR.mkdir(parents=True, exist_ok=True)

FORMATS = ["v4", "v7", "fgb"]
LABELS = {
    "v4": "sfproto v4",
    "v7": "sfproto v7",
    "fgb": "FlatGeobuf",
}

ATTR_ORDER = ["none", "few", "medium", "many"]
REGIMES = ["mixed", "geometry_heavy", "attribute_heavy"]
REGIME_TITLE = {
    "mixed": "mixed",
    "geometry_heavy": "geometry heavy",
    "attribute_heavy": "attribute heavy",
}

# =========================
# Helpers
# =========================

def parse_parts(name: str):
    """
    Expected format:
    osm_<regime>_<N>_<attr>
    where <regime> may contain underscores.
    """
    parts = name.split("_")
    if parts[0] != "osm":
        raise ValueError(f"Unexpected dataset name: {name}")

    attr = parts[-1]
    N = int(parts[-2])
    regime = "_".join(parts[1:-2])
    return regime, N, attr

# =========================
# Load & prepare data
# =========================

df = pd.read_csv(CSV_PATH)

# Parse dataset name
df[["regime", "N", "attr"]] = df["dataset"].apply(
    lambda s: pd.Series(parse_parts(s))
)

# Enforce categorical ordering
df["attr"] = pd.Categorical(df["attr"], categories=ATTR_ORDER, ordered=True)

# Compute relative sizes (explicit, robust)
for f in FORMATS:
    df[f + "_rel"] = df[f] / df["geojson"]

# Sort for plotting
df = df.sort_values(["regime", "attr", "N"])

# =========================
# Plot A: Relative size vs N
# =========================
# One figure per regime
# 2×2 panels for attribute profiles
# =========================

for regime in REGIMES:
    sub = df[df["regime"] == regime]

    fig, axes = plt.subplots(2, 2, figsize=(12, 8), sharex=True, sharey=True)
    fig.suptitle(
        f"Relative representation size vs N — {REGIME_TITLE[regime]}",
        y=0.98,
    )

    for ax, attr in zip(axes.flat, ATTR_ORDER):
        s = sub[sub["attr"] == attr]

        for f in FORMATS:
            ax.plot(
                s["N"],
                s[f + "_rel"],
                marker="o",
                label=LABELS[f],
            )

        ax.axhline(1.0, linestyle="--", linewidth=1)
        ax.set_xscale("log")
        ax.set_title(f"attributes: {attr}")
        ax.set_xlabel("N features (log)")
        ax.set_ylabel("size / GeoJSON")

    # Legend once
    axes[0, 0].legend()

    plt.tight_layout()
    out = OUT_DIR / f"plotA_rel_vs_N_{regime}.png"
    plt.savefig(out, dpi=200)
    plt.close()

# =========================
# Plot B: Attribute sensitivity
# =========================
# One figure per regime
# Largest N only
# =========================

for regime in REGIMES:
    sub = df[df["regime"] == regime]
    N_max = sub["N"].max()
    s = sub[sub["N"] == N_max].sort_values("attr")

    plt.figure(figsize=(9, 5))
    plt.title(
        f"Relative size vs attribute payload — "
        f"{REGIME_TITLE[regime]} (N={N_max})"
    )

    for f in FORMATS:
        plt.plot(
            s["attr"].astype(str),
            s[f + "_rel"],
            marker="o",
            label=LABELS[f],
        )

    plt.axhline(1.0, linestyle="--", linewidth=1)
    plt.xlabel("Attribute profile")
    plt.ylabel("size / GeoJSON")
    plt.legend()
    plt.tight_layout()

    out = OUT_DIR / f"plotB_rel_vs_attr_{regime}_N{N_max}.png"
    plt.savefig(out, dpi=200)
    plt.close()

print(f"Wrote telling plots to: {OUT_DIR.resolve()}")
