# Big Data Analytics Projects

This repository contains two Big Data Analytics course projects:

- `BigData_SocialGraph`
- `ksrtc_project`

---

## 1. BDA Social Graph Analytics

### GraphFrames vs GraphX Performance Comparison

This repository is organized around the active comparison pipeline:

1. `BigData_SocialGraph/run_comparison.py` for the main head-to-head execution
2. `BigData_SocialGraph/GraphFrames/graph_analytics.py` for the active GraphFrames implementation
3. `BigData_SocialGraph/GraphX/GraphAnalytics.scala` for the active GraphX implementation
4. `BigData_SocialGraph/dashboard_server.py` and `BigData_SocialGraph/webapp/` for the interactive frontend
5. `BigData_SocialGraph/Comparison_Report/` for generated evidence, reports, and presentation scripts

Older experimental GraphFrames modules were moved to `BigData_SocialGraph/archive/legacy_graphframes_suite/` so the main tree stays focused on the current project.

**Course:** Big Data Analytics (4th Semester)
**Dataset:** Any SNAP edge-list (auto-detected from `BigData_SocialGraph/data_lake/`)
**Current:** Facebook SNAP Combined (4,039 nodes, 88,234 edges)

---

### Quick Start - Unified Runner

Run both frameworks head-to-head on any dataset with a single command:

```cmd
cd BigData_SocialGraph

:: Auto-detect dataset in data_lake/
python run_comparison.py

:: Or specify a dataset explicitly
python run_comparison.py data_lake/facebook_combined.txt.gz
python run_comparison.py data_lake/twitter_combined.txt
python run_comparison.py data_lake/any_snap_file.txt.gz
```

This generates three presentation-ready proof artifacts in `BigData_SocialGraph/Comparison_Report/`:

- `comparison_results.txt` - plain-text side-by-side summary
- `comparison_data.json` - structured proof data for the project
- `dashboard.html` - polished frontend for class/demo presentation

### Interactive Web App

Launch the local multi-dataset frontend with:

```cmd
cd BigData_SocialGraph
python dashboard_server.py
```

Then open:

```text
http://127.0.0.1:8000
```

The web app lets you:

- pick any SNAP dataset from `BigData_SocialGraph/data_lake/`
- run GraphFrames and GraphX from the browser
- switch between saved dataset runs
- inspect latency, metric differences, proof previews, and research-backed architecture notes
- keep each dataset in `BigData_SocialGraph/Comparison_Report/runs/<dataset-slug>/` so nothing is overwritten

### Run Individually

```cmd
cd BigData_SocialGraph

:: GraphFrames only (Python)
venv\Scripts\python.exe GraphFrames\graph_analytics.py

:: GraphX only (Scala)
spark-shell -i GraphX\GraphAnalytics.scala
```

### Archived Legacy Modules

Older GraphFrames side experiments such as advanced centrality, link prediction, resilience, embeddings, and visualization exports were moved to:

```text
BigData_SocialGraph/archive/legacy_graphframes_suite/
```

They are preserved for reference, but they are not part of the active GraphFrames vs GraphX comparison flow.

### Project Structure

```text
BigData_SocialGraph/
|-- run_comparison.py                    # Unified comparison runner
|-- dashboard_server.py                  # Local API/server for the dashboard
|-- README.md
|
|-- GraphFrames/
|   |-- graph_analytics.py               # Active GraphFrames analytics pipeline
|   |-- README.md
|   +-- outputs/
|       +-- graph_analytics/             # Current GraphFrames output exports
|
|-- GraphX/
|   |-- GraphAnalytics.scala             # Active GraphX analytics pipeline
|   +-- outputs/                         # Current GraphX output exports
|
|-- data_lake/                           # SNAP datasets
|   |-- facebook_combined.txt.gz
|   +-- twitter_combined.txt.gz
|
|-- webapp/
|   |-- index.html
|   |-- styles.css
|   +-- app.js
|
|-- Comparison_Report/
|   |-- comparison_results.txt           # Latest plain-text comparison
|   |-- comparison_data.json             # Latest JSON proof bundle
|   |-- dashboard.html                   # Latest generated standalone dashboard
|   |-- reports/                         # Final report drafts
|   |-- presentations/                   # Presentation scripts
|   +-- runs/                            # Saved per-dataset evidence bundles
|
+-- archive/
    +-- legacy_graphframes_suite/        # Older side modules and outputs
```

### Algorithms Implemented

| # | Algorithm | GraphFrames | GraphX | Result Match? |
|:-:|:----------|:-----------:|:------:|:-------------:|
| 1 | PageRank | Yes | Yes | YES |
| 2 | Label Propagation | Yes | Yes | ~diff (non-deterministic) |
| 3 | Triangle Count | Yes | Yes | YES (1,612,010) |
| 4 | Connected Components | Yes | Yes | YES (1) |
| 5 | Degree Analysis | Yes | Yes | YES |
| 6 | Shortest Paths | Yes | Yes | YES |
| 7 | Clustering Coefficient | - | Yes | - |
| 8 | Graph Density | - | Yes | - |
| 9 | Community Profiling | - | Yes | - |

### Key Findings

- Both frameworks produce identical analytical results
- GraphX runs **2.4x faster** (~18s vs ~44s)
- GraphFrames is more accessible for Python teams
- GraphX has zero Python overhead and no platform bugs
- Fully dynamic: works on any SNAP edge-list dataset
- Dashboard shows proof from generated logs, metrics, timings, and source-backed differences

See `BigData_SocialGraph/Comparison_Report/reports/report.md` for the focused report and `BigData_SocialGraph/Comparison_Report/reports/final_combined_research_report.md` for the expanded final document.

### Supported Datasets

Drop any SNAP edge-list file into `BigData_SocialGraph/data_lake/` and it auto-detects:

- `.txt` / `.txt.gz` (space/tab separated)
- `.csv` / `.csv.gz` (comma separated)
- `.tsv` / `.tsv.gz` (tab separated)

Download datasets from: [SNAP Stanford](https://snap.stanford.edu/data/)

---

## 2. KSRTC Smart Transit System using Big Data Analytics

### Overview

This project implements a Big Data Analytics pipeline for KSRTC bus network analysis by combining distributed processing, graph-based route modeling, real-time data simulation, and an interactive 3D dashboard.

It demonstrates how modern data engineering techniques can be applied to smart transportation systems for route analysis, demand understanding, and transit visualization.

### Key Features

- Multi-route bus simulation across a synthetic KSRTC-style network
- Graph-based route analysis using Spark and GraphX components
- Real-time data pipeline using Kafka producers and consumers
- Traffic analytics, demand insights, and route optimization views
- 3D interactive dashboard built with Streamlit and PyDeck
- Flexible GPS handling with schema normalization and speed derivation

### Technology Stack

| Component | Technology |
| --- | --- |
| Data Processing | Apache Spark |
| Graph Processing | GraphX |
| Streaming | Kafka |
| Backend | Python + Flask |
| Database | MongoDB |
| Visualization | Streamlit + PyDeck |
| Storage | CSV, HDFS-ready pipeline |

### System Architecture

![System Architecture](ksrtc_project/visualizations/system_architecture.png)

```text
Data Sources (Routes + GPS)
        |
        v
Apache Spark Processing
        |
        v
Graph Analysis (GraphX)
        |
        v
Realtime Streaming (Kafka)
        |
        v
Backend API Layer / Result Storage
        |
        v
3D Dashboard (Streamlit + PyDeck)
```

### Project Structure

```text
ksrtc_project/
|
|-- backend/
|   |-- analysis/
|   |-- api/
|   |-- hdfs_pipeline/
|   |-- realtime/
|   |-- spark_jobs/
|   |-- run_pipeline.py
|   `-- run_pipeline.sh
|
|-- dashboard/
|   `-- dashboard.py
|
|-- data/
|   |-- raw/
|   `-- processed/
|
|-- docs/
|-- results/
|   |-- csv/
|   `-- reports/
|
|-- scripts/
|-- visualizations/
|-- requirements.txt
`-- README.md
```

### Data Description

#### Bus Routes

```text
route_id, seq, stop, lat, lon, stop_id
```

#### GPS Data

```text
bus_id, timestamp, lat, lon, speed
```

Supported data behavior:

- Multiple CSV schema styles can be normalized
- Column names are standardized automatically
- Speed can be derived from GPS coordinates when needed

### How to Run

#### 1. Install Dependencies

```bash
cd ksrtc_project
pip install -r requirements.txt
```

#### 2. Run Spark Jobs

```bash
cd ksrtc_project
spark-submit backend/spark_jobs/graph_analysis.py
```

Optional legacy/local pipeline bootstrap:

```bash
cd ksrtc_project
python backend/run_pipeline.py
```

#### 3. Run Real-Time Pipeline

```bash
cd ksrtc_project
python backend/realtime/kafka_producer.py
python backend/realtime/kafka_consumer.py
```

#### 4. Start the API Server

```bash
cd ksrtc_project
python backend/api/api_server.py
```

#### 5. Launch the Dashboard

```bash
cd ksrtc_project
streamlit run dashboard/dashboard.py
```

### Dashboard Features

- 3D interactive transit map
- Moving bus simulation
- Route visualization and stop markers
- Traffic analytics
- Speed distribution analysis
- Demand prediction view
- Route optimization insights

### Results

- Canonical analytics outputs are stored in `ksrtc_project/results/csv/`
- Final text and markdown summaries are stored in `ksrtc_project/results/reports/`
- Architecture and chart assets are stored in `ksrtc_project/visualizations/`
- The dashboard provides interactive exploration of the generated outputs

### Notes

- GPS data in this project is simulated for demonstration purposes
- The API layer is implemented for future dashboard integration
- The dashboard currently reads generated CSV outputs directly for reliable demos
- The HDFS layer is prepared for scalable storage and Spark/HDFS deployment
- `ksrtc_project/results/csv/` is the primary output folder for analysis artifacts

### Future Scope

- Real-time GPS integration with live transport feeds
- Machine learning based route optimization
- Smart city deployment workflow
- Mobile application integration

### Conclusion

This project demonstrates an end-to-end Big Data Analytics system that integrates distributed processing, graph analytics, real-time streaming, and advanced visualization.

It provides a strong foundation for intelligent transportation system experiments and smart transit decision support.
