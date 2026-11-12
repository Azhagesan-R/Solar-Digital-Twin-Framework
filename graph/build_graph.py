import os
import glob
import pandas as pd
import numpy as np
import torch
from scipy.spatial.distance import cdist

def generate_digital_twin_spatial_graph(data_directory, proximity_threshold=1.5):
    print("--- Phase 5: Commencing Graph Creation Pipeline ---")
    
    # 1. Detect partitioned plant folders inside your directory
    partition_paths = glob.glob(os.path.join(data_directory, "plant_id=*"))
    if not partition_paths:
        raise FileNotFoundError(f"No processed Parquet records found at {data_directory}")
        
    plant_ids = [int(p.split("=")[-1]) for p in partition_paths]
    plant_ids.sort() 
    print(f"Successfully mapped {len(plant_ids)} active plant nodes in the virtual twin model: {plant_ids}")
    
    # 2. Define Real Spatial Coordinates (Latitude, Longitude) for your nodes
    # These match the exact plant IDs extracted from your terminal log
    plant_coordinates = {
        4135001: [11.0168, 76.9558],  # Plant 1: Coimbatore Pooling Node
        4136001: [11.1983, 77.3410],  # Plant 2: Tiruppur Regional Node
    }
    
    # 3. Build the Spatial Adjacency Matrix (A) using Physical Distance Vectors
    coords_list = [plant_coordinates[p_id] for p_id in plant_ids]
    coords_matrix = np.array(coords_list)
    
    # Calculate geographical distances between the plants
    distance_matrix = cdist(coords_matrix, coords_matrix, metric='euclidean')
    
    # Connect sites if they are within your proximity threshold
    adjacency_matrix = (distance_matrix < proximity_threshold).astype(int)
    np.fill_diagonal(adjacency_matrix, 0) # Strip self-connections
    
    # Convert into PyTorch sparse directional index tensor format [2, Num_Edges]
    edges = np.where(adjacency_matrix == 1)
    edge_index_tensor = torch.tensor(np.vstack(edges), dtype=torch.long)
    
    # 4. Ingest Time-Series Multi-Channel Features Matrix
    node_features_list = []
    for p_id in plant_ids:
        path = os.path.join(data_directory, f"plant_id={p_id}")
        df = pd.read_parquet(path).sort_values(by="timestamp")
        
        # Pull the latest row from your preprocessed data matrix
        latest_row = df.iloc[-1]
        
        # --- PHYSICAL ASSET PARAMETERS COMPUTATION (The Digital Twin Engine) ---
        actual_ac = float(latest_row.get("actual_power_output_kw", 0.0))
        expected_ac = float(latest_row.get("expected_power_output_kw", 1.0))
        dc_power = float(latest_row.get("dc_power", actual_ac * 1.05)) # Fallback if direct DC missing
        
        # Parameter A: Live Inverter Conversion Efficiency
        inverter_efficiency = (actual_ac / max(dc_power, 1.0)) * 100.0
        inverter_efficiency = max(0.0, min(inverter_efficiency, 100.0)) # Bound between 0 and 100%
        
        # Parameter B: Panel Soiling & Degradation Index (Derived via clear-sky expectations)
        soiling_degradation_ratio = (expected_ac - actual_ac) / max(expected_ac, 1.0)
        soiling_degradation_ratio = max(0.0, min(soiling_degradation_ratio, 1.0))

        # Pack these physical attributes into the GNN input tensor slice
        # Features: [GHI, Temp, Deviation, Inverter Efficiency, Soiling Loss]
        node_features = np.array([
            latest_row.get("ghi_w_m2", 0.5),
            latest_row.get("temp_c", 0.5),
            latest_row.get("power_deviation_ratio", 0.0),
            inverter_efficiency / 100.0, # Normalized for neural network convergence
            soiling_degradation_ratio
        ])
        
        node_features_list.append(node_features)
        
    x_tensor = torch.tensor(np.array(node_features_list), dtype=torch.float)

    
    print("\n--- Spatial Grid Adjacency Matrix (A) Built ---")
    print(adjacency_matrix)
    print("\n--- Compiled ST-GNN Feature Input Tensors ---")
    print(f" -> Node Features Matrix (X) Shape [Nodes, Features]: {x_tensor.shape}")
    print(f" -> Spatial Edge Index Layout Shape [2, Directed Edges]: {edge_index_tensor.shape}")
    
    return x_tensor, edge_index_tensor

if __name__ == "__main__":
    # Point directly to your active output directory path
    DATA_PATH = "/home/azhagesan/thesis/docker/processed_solar_data"
    x, edge_index = generate_digital_twin_spatial_graph(DATA_PATH)
