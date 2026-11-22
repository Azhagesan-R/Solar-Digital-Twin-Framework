#### ☀️ AI-Powered Digital Twin Framework for Interconnected Photovoltaic (PV) Systems

An enterprise-grade, scalable **Cognitive Digital Twin** architecture integrating **Spatio-Temporal Graph Neural Networks (ST-GNNs)** with real-time big data streaming (**Apache Kafka & Spark**) for short-term solar power forecasting and multi-threat anomaly detection. 

#### 📌 Project Overview

Modern power grids face severe stability challenges due to the intermittency of distributed solar energy. Traditional forecasting frameworks rely solely on isolated time-series data, failing to capture how localized weather patterns (such as moving cloud covers) propagate geographically across interconnected solar stations. Furthermore, standard monitoring tools lack the real-time infrastructure required to simultaneously isolate hardware degradation, sensor malfunctions, and commercial power theft. 

This project resolves these gaps by establishing a closed-loop cyber-physical **Digital Twin Framework**: 

* **Physics-Informed Baseline (pvlib):** Simulates real-world thermodynamic panel degradation, inverter conversion efficiency, and panel soiling loss indexes.
* **Spatio-Temporal Intelligence (ST-GNN):** Combines Graph Convolutional Networks (GCN) with Recurrent Neural Units (GRU/LSTM) to model geographical correlations alongside historical weather trends.
* **Near-Real-Time Pipeline:** Uses containerized **Apache Kafka** and **Apache Spark** to ingest, clean, and process high-frequency SCADA telemetry streams.
* **Interactive Monitoring UI:** Features a live **Streamlit** operational dashboard mapping active nodes, weather conditions, and automated anomaly alerts.

#### 🏗️ System Architecture

The framework operates across five integrated layers: 

1. **Data Streams Layer:** Ingests telemetry from interconnected PV sites, weather stations (GHI, ambient temperature, wind speed), inverter sensors, and local grid logs.
2. **Streaming Infrastructure Layer:** Containerized Apache Kafka message brokers and Apache Spark engines handle distributed ingestion, window-based time-series imputation (LOCF), and Min-Max feature scaling.
3. **Digital Twin Layer:** Synthesizes virtual asset shadows, computing real-time State-of-Health (SoH) metrics (Inverter Efficiency & Soiling Loss) and constructing dynamic spatial adjacency matrices.
4. **AI Analytics Core (ST-GNN):** 

  * *Forecasting Head:* Predicts short-term power generation to maintain grid stability and avoid Deviation Settlement Mechanism (DSM) penalties.
  * *Anomaly Detection Head:* Identifies False Data Injection (FDI) attacks, replay attacks, localized grid failures, and physical power theft.
5. **Outputs & Applications:** Delivers live visual analytics, grid-operator alerts, and predictive maintenance insights via the Streamlit dashboard.

#### 📂 Repository Structure

Show Code

Hide Code

Download diagram SVG

ASCII

Solar-Digital-Twin-Framework/
├── docker/
│   ├── docker-compose.yml       # Multi-container orchestration (Spark, Kafka, Zookeeper)
│   └── spark.dockerfile         # Custom Apache Spark environment configuration
├── data_preprocessing/
│   └── preprocess.py            # PySpark time-series imputation, physics features & scaling
├── graph/
│   └── build_graph.py           # Spatial adjacency matrix & PyTorch tensor generator
├── dashboard/
│   └── dashboard_app.py         # Real-time Streamlit Digital Twin monitoring UI
├── datasets/                    # Directory structure for UNISOLAR, Weather & Anomaly logs
└── README.md                    # Project documentation

Use code with caution.

Copy code text to clipboard.

#### 🛠️ Tech Stack

* **Deep Learning & Graph Processing:** PyTorch, PyTorch Geometric, NetworkX, SciPy
* **Physics & Solar Modeling:** pvlib-python
* **Big Data & Streaming:** Apache Spark (PySpark), Apache Kafka, Zookeeper
* **Containerization:** Docker, Docker Compose
* **Frontend & Visualization:** Streamlit, Pandas, NumPy

#### 🚀 Quickstart & Execution Guide

#### 1. Spin Up the Streaming & Processing Infrastructure

Navigate to the docker directory and launch the containerized Spark, Kafka, and Zookeeper services: 

bash

cd docker
docker compose up -d --build

Use code with caution.

Copy code text to clipboard.

#### 2. Run PySpark Data Preprocessing

Execute the distributed preprocessing pipeline inside the running solar-spark container to clean telemetry logs and generate Parquet partitions: 

bash

docker exec -it solar-spark /opt/spark/bin/spark-submit /opt/project/src/preprocess.py

Use code with caution.

Copy code text to clipboard.

#### 3. Build the Spatio-Temporal Graph Tensors

Extract processed partitions to compute spatial Euclidean distances, construct the Adjacency Matrix (A), and generate PyTorch feature tensors: 

bash

cd ../graph
python3 build_graph.py

Use code with caution.

Copy code text to clipboard.

#### 4. Launch the Digital Twin Monitoring Dashboard

Start the interactive web interface to monitor spatial correlations, live weather states, inverter efficiency, and anomaly alerts: 

bash

cd ../dashboard
streamlit run dashboard_app.py

Use code with caution.

Copy code text to clipboard.

#### 📊 Key Outcomes & Grid Benefits

* **Outage Prevention:** Proactive cloud-propagation tracking allows grid operators to ramp up spinning reserves hours before sudden solar ramp-down events.
* **Grid Stability & Compliance:** Reduces forecasting error margins to help solar operators stay within strict regulatory tolerance bands (e.g., CERC DSM guidelines).
* **Reduced Operational Costs:** Pinpoints exact node-level hardware degradation and power theft automatically, minimizing manual field inspections.