"""Stage 6 - exploratory data analysis and visualization.

Input : data/output.db (the SQLite table written by loader.py - i.e. the data
        that really went through the whole pipeline) and
        data/raw/gbif_year_counts.json (GBIF totals per year from the collector)
Output: figures/*.png and data/summaries/eda_summary.txt

Research questions
  Q1  Which taxonomic groups dominate biodiversity records in Kazakhstan, and
      how does the composition change through the seasons?
  Q2  Where in the country are records concentrated and which regions have the
      highest species richness?
  Q3  How many threatened (IUCN VU/EN/CR) species appear, and where?
  Q4  How precise are the coordinates of different data sources?
  Q5  How fast is the number of GBIF records from Kazakhstan growing?

Run:  python -m src.analysis
"""

import io
import json
import logging
import sqlite3
from contextlib import redirect_stdout

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import seaborn as sns  # noqa: E402

from src.config import settings  # noqa: E402
from src.stats import update_stats  # noqa: E402

log = logging.getLogger("analysis")

# fixed categorical order (colour follows the entity, never its rank)
PALETTE = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
OTHER_GREY = "#a3a29b"
INK, INK_2, GRID = "#0b0b0b", "#52514e", "#e4e3df"
SEASONS = ["Winter", "Spring", "Summer", "Autumn"]


def setup_style() -> None:
    sns.set_theme(style="whitegrid")
    plt.rcParams.update({
        "figure.facecolor": "#fcfcfb", "axes.facecolor": "#fcfcfb", "savefig.facecolor": "#fcfcfb",
        "axes.edgecolor": GRID, "grid.color": GRID, "grid.linewidth": 0.8,
        "axes.labelcolor": INK_2, "xtick.color": INK_2, "ytick.color": INK_2, "text.color": INK,
        "axes.titleweight": "bold", "axes.titlesize": 13, "axes.titlelocation": "left",
        "axes.spines.top": False, "axes.spines.right": False, "legend.frameon": False,
        "font.size": 10, "figure.dpi": 110,
    })


def load_from_db() -> pd.DataFrame:
    with sqlite3.connect(settings.sqlite_db_path) as conn:
        df = pd.read_sql_query("SELECT * FROM occurrences", conn, parse_dates=["event_date"])
    for col in ["kingdom", "taxon_class", "basis_of_record", "iucn_category", "region", "season",
                "precision_class", "dataset_name"]:
        df[col] = df[col].astype("category")
    df["season"] = df["season"].cat.reorder_categories(SEASONS, ordered=True)
    for col in ["identified_to_species", "is_threatened", "uncertainty_imputed", "count_imputed", "is_high_quality"]:
        df[col] = df[col].astype(bool)  # SQLite stores booleans as 0/1
    return df


def save(fig: plt.Figure, name: str) -> str:
    settings.figures_dir.mkdir(parents=True, exist_ok=True)
    path = settings.figures_dir / f"{name}.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    log.info("saved %s", path)
    return path.name


# ---------------------------------------------------------------------------
# text EDA
# ---------------------------------------------------------------------------
def describe(df: pd.DataFrame) -> dict:
    print("=== 1. DIMENSIONS ===")
    print(f"rows = {df.shape[0]}, columns = {df.shape[1]}")

    print("\n=== 2. DATA TYPES ===")
    print(df.dtypes.astype(str).str.replace(r"category.*", "category", regex=True).value_counts().to_string())
    
    print("\n=== 3. DESCRIPTIVE STATISTICS (numerical) ===")
    num_cols = ["latitude", "longitude", "coord_uncertainty_m", "individual_count", "dist_almaty_km",
                "dist_center_km", "issue_count", "media_count"]
    print(df[num_cols].describe().T.round(2).to_string())
    print("\nskewness:\n", df[num_cols].skew().round(2).to_string())
    
    print("\n=== 4. CATEGORICAL STATISTICS ===")
    cat_cols = ["kingdom", "taxon_class", "basis_of_record", "iucn_category", "region", "season", "dataset_name"]
    print(df[cat_cols].describe().T.to_string())
    for col in ["kingdom", "basis_of_record", "iucn_category", "precision_class"]:
        vc = df[col].value_counts()
        print(f"\n{col}:\n" + pd.DataFrame({"n": vc, "%": (vc / len(df) * 100).round(1)}).to_string())
    print(f"\nunique species: {df['species'].nunique()}, families: {df['family'].nunique()}, "
          f"orders: {df['taxon_order'].nunique()}")

    print("\n=== 5. GROUPED ANALYSIS ===")
    by_class = (df.groupby("taxon_class", observed=True)
                  .agg(records=("gbif_id", "size"), species=("species", "nunique"),
                       median_unc_m=("coord_uncertainty_m", "median"),
                       share_high_quality=("is_high_quality", "mean"))
                  .sort_values("records", ascending=False).head(8))
    by_class["share_high_quality"] = (by_class["share_high_quality"] * 100).round(1)
    print(by_class.to_string())
    season_share = pd.crosstab(df["season"], df["kingdom"], normalize="index").mul(100).round(1)
    print("\nkingdom share per season (%):\n", season_share.to_string())
    by_dataset = (df.groupby("dataset_name", observed=True)["coord_uncertainty_m"]
                    .agg(["count", "median", "mean"]).sort_values("count", ascending=False).round(1))
    print("\ncoordinate uncertainty by dataset (m):\n", by_dataset.to_string())
    corr = df[["latitude", "longitude", "dist_almaty_km", "log10_uncertainty", "issue_count"]].corr().round(2)
    print("\ncorrelation matrix:\n", corr.to_string())
    return {"rows": int(df.shape[0]), "columns": int(df.shape[1]),
            "species": int(df["species"].nunique()),
            "threatened_records": int(df["is_threatened"].sum()),
            "share_birds_pct": round(float((df["taxon_class"] == "Aves").mean() * 100), 1)}


# ---------------------------------------------------------------------------
# figures
# ---------------------------------------------------------------------------
def fig_season_composition(df: pd.DataFrame) -> str:
    """Q1: top-5 classes per month (sample is stratified: equal records per month)."""
    top = df["taxon_class"].value_counts().head(5).index.tolist()
    sub = df.assign(group=np.where(df["taxon_class"].isin(top), df["taxon_class"].astype(str), "Other"))
    table = pd.crosstab(sub["month"], sub["group"], normalize="index").mul(100)[top + ["Other"]]
    fig, ax = plt.subplots(figsize=(10, 5.2))
    bottom = np.zeros(len(table))
    colors = PALETTE[:len(top)] + [OTHER_GREY]
    for col, color in zip(table.columns, colors):
        ax.bar(table.index, table[col], bottom=bottom, color=color, label=col, width=0.8,
               edgecolor="#fcfcfb", linewidth=1.5)
        bottom += table[col].to_numpy()
    ax.set_xticks(range(1, 13), ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])
    ax.set_ylabel("Share of records in month (%)")
    ax.set_xlabel("Month of observation")
    ax.set_ylim(0, 100)
    ax.set_title("Figure 1. Taxonomic composition of records by month (2015-2025)")
    ax.legend(title="Class", bbox_to_anchor=(1.01, 1), loc="upper left")
    return save(fig, "fig1_monthly_class_composition")


def fig_region_richness(df: pd.DataFrame) -> str:
    """Q2: species richness per region."""
    reg = (df[df["region"] != "Unknown"].groupby("region", observed=True)
             .agg(records=("gbif_id", "size"), species=("species", "nunique"))
             .sort_values("species"))
    fig, ax = plt.subplots(figsize=(9, 6))
    ax.barh(reg.index.astype(str), reg["species"], color=PALETTE[0], height=0.7)
    for y, (s, r) in enumerate(zip(reg["species"], reg["records"])):
        ax.text(s + 2, y, f"{s} spp. / {r} rec.", va="center", fontsize=8, color=INK_2)
    ax.set_xlabel("Number of distinct species")
    ax.set_ylabel("Region")
    ax.set_xlim(0, reg["species"].max() * 1.25)
    ax.set_title("Figure 2. Species richness by region")
    ax.grid(axis="y", visible=False)
    return save(fig, "fig2_region_species_richness")


def fig_map(df: pd.DataFrame) -> str:
    """Q2: geographic distribution of records by kingdom."""
    kingdoms = ["Animalia", "Plantae"]
    fig, ax = plt.subplots(figsize=(10, 6))
    other = df[~df["kingdom"].isin(kingdoms)]
    for k, color in zip(kingdoms, PALETTE):
        part = df[df["kingdom"] == k]
        ax.scatter(part["longitude"], part["latitude"], s=14, color=color, alpha=0.65, label=f"{k} ({len(part)})",
                   edgecolors="#fcfcfb", linewidths=0.4)
    ax.scatter(other["longitude"], other["latitude"], s=22, color=PALETTE[3], marker="D",
               label=f"Fungi / Bacteria / Viruses ({len(other)})", edgecolors="#fcfcfb", linewidths=0.4)
    for name, (lat, lon) in {"Almaty": (43.2389, 76.8897), "Astana": (51.1694, 71.4491),
                             "Atyrau": (47.0945, 51.9238), "Shymkent": (42.3417, 69.5901)}.items():
        ax.plot(lon, lat, marker="*", color=INK, markersize=10)
        ax.annotate(name, (lon, lat), xytext=(5, 5), textcoords="offset points", fontsize=9, color=INK)
    ax.set_xlim(46, 88)
    ax.set_ylim(40, 56)
    ax.set_xlabel("Longitude (°E)")
    ax.set_ylabel("Latitude (°N)")
    ax.set_aspect(1.4)
    ax.set_title("Figure 3. Location of occurrence records in Kazakhstan")
    ax.legend(loc="upper left", title="Kingdom (records)")
    return save(fig, "fig3_occurrence_map")


def fig_threatened(df: pd.DataFrame) -> str:
    """Q3: IUCN categories and threatened species."""
    order = ["LC", "NT", "VU", "EN", "CR", "DD", "NE"]
    counts = df["iucn_category"].value_counts().reindex(order, fill_value=0)
    thr = (df[df["is_threatened"]].groupby(["species", "iucn_category"], observed=True)
             .size().rename("records").reset_index().sort_values("records"))
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.8), gridspec_kw={"width_ratios": [1, 1.3]})
    colors = [PALETTE[0] if c not in ("VU", "EN", "CR") else PALETTE[7] for c in counts.index]
    ax1.bar(counts.index, counts.values, color=colors, width=0.7)
    for x, v in enumerate(counts.values):
        ax1.text(x, v + 8, str(v), ha="center", fontsize=9, color=INK_2)
    ax1.set_xlabel("IUCN Red List category")
    ax1.set_ylabel("Records")
    ax1.set_title("Figure 4a. Records by IUCN category")
    ax1.grid(axis="x", visible=False)
    cat_color = {"VU": PALETTE[3], "EN": PALETTE[7], "CR": PALETTE[6]}
    ax2.barh(thr["species"], thr["records"], color=[cat_color[c] for c in thr["iucn_category"]], height=0.65)
    for y, (r, c) in enumerate(zip(thr["records"], thr["iucn_category"])):
        ax2.text(r + 0.2, y, c, va="center", fontsize=8, color=INK_2)
    ax2.set_xlabel("Records")
    ax2.set_title("Figure 4b. Threatened species (VU / EN / CR)")
    ax2.grid(axis="y", visible=False)
    for lbl in ax2.get_yticklabels():
        lbl.set_fontstyle("italic")
    from matplotlib.patches import Patch
    ax2.legend(handles=[Patch(color=cat_color[c], label=c) for c in ("VU", "EN") if c in set(thr["iucn_category"])],
               title="Category", loc="lower right")
    fig.tight_layout()
    return save(fig, "fig4_iucn_threatened")


def fig_uncertainty(df: pd.DataFrame) -> str:
    """Q4: coordinate precision by data source (reported values only, imputed excluded)."""
    rep = df[~df["uncertainty_imputed"].astype(bool)].copy()
    top = rep["dataset_name"].value_counts().head(4).index
    rep = rep[rep["dataset_name"].isin(top)]
    rep["dataset_name"] = rep["dataset_name"].astype(str)
    order = rep.groupby("dataset_name")["coord_uncertainty_m"].median().sort_values().index.tolist()
    fig, ax = plt.subplots(figsize=(10, 4.8))
    sns.boxplot(data=rep, x="coord_uncertainty_m", y="dataset_name", order=order, ax=ax,
                color=PALETTE[0], fliersize=3, width=0.55, log_scale=True,
                boxprops={"alpha": 0.85}, medianprops={"color": INK})
    for y, name in enumerate(order):
        n = (rep["dataset_name"] == name).sum()
        ax.text(1.01, y, f"n={n}", transform=ax.get_yaxis_transform(), va="center", fontsize=8, color=INK_2)
    ax.axvline(1000, color=PALETTE[1], linestyle="--", linewidth=1.5, label="1 km quality threshold")
    ax.set_xlabel("Coordinate uncertainty, metres (log scale)")
    ax.set_ylabel("Dataset")
    ax.set_title("Figure 5. Coordinate precision by data source (reported values only)")
    ax.legend(loc="lower right")
    return save(fig, "fig5_uncertainty_by_dataset")


def fig_growth() -> str | None:
    """Q5: total GBIF records for KZ per year (from the collector's facet query)."""
    if not settings.year_facets_json.exists():
        return None
    counts = pd.Series(json.loads(settings.year_facets_json.read_text())).rename(int).sort_index()
    growth = counts.pct_change().mul(100)
    fig, ax = plt.subplots(figsize=(10, 4.8))
    ax.bar(counts.index, counts.values / 1000, color=PALETTE[0], width=0.7)
    for x, v, g in zip(counts.index, counts.values, growth.values):
        label = f"{v / 1000:.0f}k" + ("" if np.isnan(g) else f"\n{g:+.0f}%")
        ax.text(x, v / 1000 + counts.max() / 1000 * 0.015, label, ha="center", fontsize=8, color=INK_2)
    ax.set_xticks(counts.index)
    ax.set_xlabel("Year")
    ax.set_ylabel("Records with coordinates (thousands)")
    ax.set_ylim(0, counts.max() / 1000 * 1.18)
    ax.set_title("Figure 6. All GBIF occurrence records from Kazakhstan per year")
    ax.grid(axis="x", visible=False)
    return save(fig, "fig6_gbif_records_per_year")


def analyse() -> dict:
    setup_style()
    df = load_from_db()
    buffer = io.StringIO()
    with redirect_stdout(buffer):
        facts = describe(df)
    text = buffer.getvalue()
    print(text)
    settings.summary_dir.mkdir(parents=True, exist_ok=True)
    (settings.summary_dir / "eda_summary.txt").write_text(text, encoding="utf-8")

    figures = [fig_season_composition(df), fig_region_richness(df), fig_map(df),
               fig_threatened(df), fig_uncertainty(df), fig_growth()]
    figures = [f for f in figures if f]
    update_stats("analysis", {**facts, "figures": figures})
    print(f"\nSaved {len(figures)} figures to {settings.figures_dir}")
    return facts


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    analyse()
