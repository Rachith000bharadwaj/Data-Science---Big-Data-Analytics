"""
graph_analytics.py
==================
Dynamic Social Graph Analytics Engine using PySpark GraphFrames.

Automatically detects ANY SNAP edge-list dataset placed in data_lake/
and runs the full analytics pipeline:
  1. PageRank           — Influence maximization
  2. Label Propagation  — Community detection
  3. Triangle Count     — Clustering coefficient
  4. Connected Components — Graph connectivity
  5. Shortest Paths     — Reachability from landmark nodes
  6. Degree Distribution — In/Out/Total degree analysis

Usage:
  python graph_analytics.py                       # auto-detect dataset
  python graph_analytics.py data_lake/myfile.txt   # explicit dataset
  python graph_analytics.py data_lake/myfile.txt.gz # compressed OK

Author  : BDA Social Graph Project
"""

import os
import sys
import glob
import time
import pyspark

# -- Windows Environment Setup --------------------------
os.environ['SPARK_HOME'] = os.path.dirname(pyspark.__file__)
os.environ["PYSPARK_PIN_THREAD"] = "true"
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
hadoop_home = r'C:\hadoop'
if os.path.isdir(hadoop_home):
    os.environ['HADOOP_HOME'] = hadoop_home
# --------------------------------------------------------

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.types import LongType
from graphframes import GraphFrame

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(os.path.dirname(BASE_DIR), "data_lake")
OUT_DIR  = os.path.join(BASE_DIR, "outputs", "graph_analytics")


# =======================================================
#  1. AUTO-DETECT SNAP DATASET
# =======================================================
def discover_dataset(explicit_path=None):
    """
    If explicit_path given, use it.
    Otherwise scan data_lake/ for the first .txt or .txt.gz edge-list.
    """
    if explicit_path and os.path.exists(explicit_path):
        return explicit_path

    # Scan data_lake for edge-list files
    candidates = (
        glob.glob(os.path.join(DATA_DIR, "*.txt"))
        + glob.glob(os.path.join(DATA_DIR, "*.txt.gz"))
        + glob.glob(os.path.join(DATA_DIR, "*.csv"))
        + glob.glob(os.path.join(DATA_DIR, "*.csv.gz"))
        + glob.glob(os.path.join(DATA_DIR, "*.tsv"))
        + glob.glob(os.path.join(DATA_DIR, "*.tsv.gz"))
    )
    if not candidates:
        print("[ERROR] No dataset found in data_lake/")
        print("  Place a SNAP edge-list file (e.g. facebook_combined.txt.gz)")
        print(f"  into: {DATA_DIR}")
        sys.exit(1)

    # Sort by size (prefer smaller for faster runs), take first
    candidates.sort(key=lambda f: os.path.getsize(f))
    chosen = candidates[0]
    return chosen


def detect_dataset_name(path):
    """Extract a human-readable dataset name from the file path."""
    name = os.path.basename(path)
    # Strip extensions
    for ext in ['.txt.gz', '.csv.gz', '.tsv.gz', '.txt', '.csv', '.tsv']:
        if name.endswith(ext):
            name = name[:-len(ext)]
            break
    return name.replace('_', ' ').replace('-', ' ').title()


# =======================================================
#  2. SPARK SESSION FACTORY
# =======================================================
def create_spark_session(app_name="SocialGraphAnalytics"):
    """Creates a SparkSession with GraphFrames JAR configured."""
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages",
                "graphframes:graphframes:0.8.3-spark3.5-s_2.12")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# =======================================================
#  3. DYNAMIC GRAPH LOADER
# =======================================================
def load_graph(spark, data_path):
    """
    Loads ANY SNAP edge-list into a GraphFrame.
    Handles:
      - .txt / .txt.gz  (space/tab separated)
      - .csv / .csv.gz  (comma separated)
      - .tsv / .tsv.gz  (tab separated)
      - Lines starting with # are treated as comments and skipped.
    Returns: GraphFrame (undirected — edges in both directions)
    """
    raw = spark.read.text(data_path)

    # Filter comment lines (SNAP datasets use # for headers)
    raw = raw.filter(~F.col("value").startswith("#"))

    # Detect separator: if commas present, use comma; else whitespace
    # We'll try splitting by whitespace (handles both space and tab)
    edges_df = (
        raw.select(
            F.split(F.col("value"), r"[\s,\t]+").alias("parts")
        )
        .filter(F.size("parts") >= 2)
        .select(
            F.col("parts")[0].cast(LongType()).alias("src"),
            F.col("parts")[1].cast(LongType()).alias("dst"),
        )
        .filter(F.col("src").isNotNull() & F.col("dst").isNotNull())
        .filter(F.col("src") != F.col("dst"))  # remove self-loops
    )

    # Make undirected: add reverse edges, then deduplicate
    reverse = edges_df.select(
        F.col("dst").alias("src"),
        F.col("src").alias("dst"),
    )
    all_edges = edges_df.union(reverse).distinct()

    # Build vertices from unique node IDs
    v1 = all_edges.select(F.col("src").alias("id"))
    v2 = all_edges.select(F.col("dst").alias("id"))
    vertices = v1.union(v2).distinct()

    g = GraphFrame(vertices, all_edges)
    return g


# =======================================================
#  4. ANALYTICS MODULES
# =======================================================

def run_pagerank(g, max_iter=5):
    """PageRank — identifies most influential nodes."""
    print("\n" + "=" * 55)
    print("  MODULE 1: PAGERANK -- Influence Maximization")
    print("=" * 55)
    pr = g.pageRank(resetProbability=0.15, maxIter=max_iter)

    top = pr.vertices.orderBy(F.col("pagerank").desc())
    print("\n  Top 10 Most Influential Nodes:")
    top.show(10, truncate=False)
    return pr


def run_label_propagation(g, max_iter=5):
    """Label Propagation Algorithm — community detection."""
    print("\n" + "=" * 55)
    print("  MODULE 2: LABEL PROPAGATION -- Community Detection")
    print("=" * 55)
    communities = g.labelPropagation(maxIter=max_iter)

    comm_sizes = (
        communities.groupBy("label")
        .count()
        .orderBy(F.col("count").desc())
    )
    n_communities = comm_sizes.count()
    print(f"\n  Total Communities Detected: {n_communities}")
    print("\n  Top 10 Largest Communities:")
    comm_sizes.show(10, truncate=False)
    return communities


def run_triangle_count(g):
    """Triangle Count — measures clustering coefficient."""
    print("\n" + "=" * 55)
    print("  MODULE 3: TRIANGLE COUNT -- Clustering Coefficient")
    print("=" * 55)
    tc = g.triangleCount()

    total_triangles = tc.agg(F.sum("count")).first()[0] // 3
    print(f"\n  Total Unique Triangles: {total_triangles:,}")

    top_tc = tc.orderBy(F.col("count").desc())
    print("\n  Top 10 Nodes by Triangle Participation:")
    top_tc.show(10, truncate=False)
    return tc


def run_connected_components(spark, g):
    """Connected Components — graph connectivity analysis."""
    print("\n" + "=" * 55)
    print("  MODULE 4: CONNECTED COMPONENTS -- Connectivity")
    print("=" * 55)

    # Checkpoint required for connected components
    checkpoint_dir = os.path.join(BASE_DIR, "checkpoints")
    os.makedirs(checkpoint_dir, exist_ok=True)
    spark.sparkContext.setCheckpointDir(checkpoint_dir)

    cc = g.connectedComponents()

    comp_sizes = (
        cc.groupBy("component")
        .count()
        .orderBy(F.col("count").desc())
    )
    n_components = comp_sizes.count()
    largest = comp_sizes.first()["count"]
    total_nodes = cc.count()

    print(f"\n  Total Connected Components: {n_components}")
    print(f"  Largest Component Size:     {largest:,} / {total_nodes:,} "
          f"({largest/total_nodes*100:.1f}%)")
    print("\n  Top 5 Components:")
    comp_sizes.show(5, truncate=False)
    return cc


def run_degree_analysis(g):
    """Degree Distribution — in-degree, out-degree, total degree."""
    print("\n" + "=" * 55)
    print("  MODULE 5: DEGREE ANALYSIS -- Connectivity Distribution")
    print("=" * 55)

    degrees = g.degrees
    in_degrees = g.inDegrees
    out_degrees = g.outDegrees

    # Stats
    stats = degrees.agg(
        F.avg("degree").alias("avg_degree"),
        F.max("degree").alias("max_degree"),
        F.min("degree").alias("min_degree"),
        F.stddev("degree").alias("std_degree"),
    ).first()

    print(f"\n  Average Degree:  {stats['avg_degree']:.2f}")
    print(f"  Max Degree:      {stats['max_degree']}")
    print(f"  Min Degree:      {stats['min_degree']}")
    print(f"  Std Dev:         {stats['std_degree']:.2f}")

    print("\n  Top 10 Most Connected Nodes:")
    degrees.orderBy(F.col("degree").desc()).show(10, truncate=False)

    # Degree distribution (binned)
    print("  Degree Distribution (top 15 bins):")
    deg_dist = (
        degrees.groupBy("degree")
        .count()
        .orderBy(F.col("count").desc())
    )
    deg_dist.show(15, truncate=False)

    return degrees, in_degrees, out_degrees, stats


def run_shortest_paths(g, landmarks=None):
    """Shortest Paths — BFS from landmark nodes."""
    print("\n" + "=" * 55)
    print("  MODULE 6: SHORTEST PATHS -- Reachability Analysis")
    print("=" * 55)

    if landmarks is None:
        # Auto-pick top-3 highest-degree nodes as landmarks
        top_nodes = (
            g.degrees.orderBy(F.col("degree").desc())
            .limit(3)
            .select("id")
            .collect()
        )
        landmarks = [row["id"] for row in top_nodes]

    print(f"  Landmark Nodes: {landmarks}")
    sp = g.shortestPaths(landmarks=landmarks)

    print("\n  Shortest Path Distances (sample of 10 nodes):")
    sp.select("id", "distances").show(10, truncate=False)
    return sp


# =======================================================
#  5. CSV EXPORT
# =======================================================
def export_results(pr, communities, tc, cc, degrees, dataset_name):
    """Exports all results as CSVs to outputs/graph_analytics/."""
    os.makedirs(OUT_DIR, exist_ok=True)

    print("\n" + "=" * 55)
    print("  EXPORTING RESULTS TO CSV")
    print("=" * 55)

    # PageRank top 100
    pr_top = pr.vertices.orderBy(F.col("pagerank").desc()).limit(100)
    pr_path = os.path.join(OUT_DIR, "pagerank_top100")
    pr_top.coalesce(1).write.mode("overwrite").csv(pr_path, header=True)
    print(f"  [OK] PageRank Top 100       -> {pr_path}")

    # Community assignments
    comm_path = os.path.join(OUT_DIR, "community_assignments")
    communities.coalesce(1).write.mode("overwrite").csv(comm_path, header=True)
    print(f"  [OK] Community Assignments  -> {comm_path}")

    # Triangle counts
    tc_path = os.path.join(OUT_DIR, "triangle_counts")
    tc.orderBy(F.col("count").desc()).limit(100).coalesce(1).write.mode("overwrite").csv(tc_path, header=True)
    print(f"  [OK] Triangle Counts Top100 -> {tc_path}")

    # Connected components
    cc_path = os.path.join(OUT_DIR, "connected_components")
    cc_summary = cc.groupBy("component").count().orderBy(F.col("count").desc())
    cc_summary.coalesce(1).write.mode("overwrite").csv(cc_path, header=True)
    print(f"  [OK] Connected Components   -> {cc_path}")

    # Degree distribution
    deg_path = os.path.join(OUT_DIR, "degree_distribution")
    degrees.orderBy(F.col("degree").desc()).coalesce(1).write.mode("overwrite").csv(deg_path, header=True)
    print(f"  [OK] Degree Distribution    -> {deg_path}")

    print(f"\n  All outputs saved under: {OUT_DIR}")


def export_benchmarks_and_summary(
    dataset_name,
    timings,
    n_vertices,
    n_edges,
    community_count,
    component_count,
    total_triangles,
    degree_stats,
):
    """Exports timing and summary CSVs aligned with the GraphX outputs."""
    os.makedirs(OUT_DIR, exist_ok=True)

    bench_path = os.path.join(OUT_DIR, "benchmark_timings.csv")
    with open(bench_path, "w", encoding="utf-8") as handle:
        handle.write("algorithm,time_seconds\n")
        for name, seconds in timings.items():
            handle.write(f"{name},{seconds:.4f}\n")

    summary_path = os.path.join(OUT_DIR, "graph_summary.csv")
    with open(summary_path, "w", encoding="utf-8") as handle:
        handle.write("metric,value\n")
        handle.write(f"dataset,{dataset_name}\n")
        handle.write(f"nodes,{n_vertices}\n")
        handle.write(f"edges,{n_edges}\n")
        handle.write(f"triangles,{total_triangles}\n")
        handle.write(f"communities_lpa,{community_count}\n")
        handle.write(f"connected_components,{component_count}\n")
        handle.write(f"max_degree,{degree_stats['max_degree']}\n")
        handle.write(f"min_degree,{degree_stats['min_degree']}\n")
        handle.write(f"avg_degree,{degree_stats['avg_degree']:.4f}\n")
        handle.write(f"std_degree,{degree_stats['std_degree']:.4f}\n")
        handle.write(f"total_time_seconds,{timings.get('TOTAL', 0.0):.4f}\n")

    print(f"  [OK] Benchmark Timings      -> {bench_path}")
    print(f"  [OK] Graph Summary          -> {summary_path}")


# =======================================================
#  MAIN
# =======================================================
if __name__ == "__main__":
    # Accept optional CLI argument for dataset path
    explicit = sys.argv[1] if len(sys.argv) > 1 else None
    if explicit and not os.path.isabs(explicit):
        explicit = os.path.join(BASE_DIR, explicit)

    data_path = discover_dataset(explicit)
    dataset_name = detect_dataset_name(data_path)
    file_size_mb = os.path.getsize(data_path) / (1024 * 1024)

    print("=" * 55)
    print("  BDA Social Graph -- Dynamic GraphFrames Analytics")
    print("=" * 55)
    print(f"\n  Dataset:  {dataset_name}")
    print(f"  File:     {os.path.basename(data_path)}")
    print(f"  Size:     {file_size_mb:.1f} MB")

    total_start = time.time()
    timings = {}

    spark = create_spark_session(f"SocialGraph_{dataset_name.replace(' ', '_')}")
    print(f"  Spark:    v{spark.version}")

    try:
        # -- Load Graph --
        print("\n" + "-" * 55)
        print("  LOADING GRAPH FROM SNAP DATASET...")
        print("-" * 55)

        load_start = time.time()
        g = load_graph(spark, data_path)
        n_vertices = g.vertices.count()
        n_edges = g.edges.count() // 2  # undirected
        timings["Graph Loading"] = time.time() - load_start

        print(f"\n  [OK] Graph loaded successfully!")
        print(f"    Nodes: {n_vertices:,}")
        print(f"    Edges: {n_edges:,} (undirected)")

        # -- Run All Analytics --
        step_start = time.time()
        pr = run_pagerank(g, max_iter=5)
        timings["PageRank"] = time.time() - step_start

        step_start = time.time()
        communities = run_label_propagation(g, max_iter=5)
        timings["Label Propagation"] = time.time() - step_start

        step_start = time.time()
        tc = run_triangle_count(g)
        timings["Triangle Count"] = time.time() - step_start

        step_start = time.time()
        cc = run_connected_components(spark, g)
        timings["Connected Components"] = time.time() - step_start

        step_start = time.time()
        degrees, in_deg, out_deg, degree_stats = run_degree_analysis(g)
        timings["Degree Analysis"] = time.time() - step_start

        step_start = time.time()
        sp = run_shortest_paths(g)
        timings["Shortest Paths"] = time.time() - step_start

        total_triangles = tc.agg(F.sum("count")).first()[0] // 3
        community_count = communities.select("label").distinct().count()
        component_count = cc.select("component").distinct().count()

        # -- Export Results --
        export_results(pr, communities, tc, cc, degrees, dataset_name)
        timings["TOTAL"] = time.time() - total_start
        export_benchmarks_and_summary(
            dataset_name,
            timings,
            n_vertices,
            n_edges,
            community_count,
            component_count,
            total_triangles,
            degree_stats,
        )

        # -- Summary --
        print("\n" + "=" * 55)
        print("                  ANALYSIS COMPLETE")
        print("=" * 55)
        print(f"  Dataset:              {dataset_name}")
        print(f"  Nodes:                {n_vertices:,}")
        print(f"  Edges:                {n_edges:,}")
        print(f"  Communities (LPA):    {community_count}")
        print(f"  Connected Components: {component_count}")
        print(f"  Total Triangles:      {total_triangles:,}")
        print(f"  Max Degree:           {degree_stats['max_degree']}")
        print(f"  Average Degree:       {degree_stats['avg_degree']:.2f}")
        print("\n  Per-Algorithm Timings:")
        print("  " + "-" * 45)
        for name, seconds in timings.items():
            print(f"    {name:<25} {seconds:>8.2f} s")
        print("  " + "-" * 45)

    except Exception as e:
        print(f"\n[ERROR] Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        raise

    finally:
        spark.stop()
        print("\n  Spark session stopped cleanly.")
