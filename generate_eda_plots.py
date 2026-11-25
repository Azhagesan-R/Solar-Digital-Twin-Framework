import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Clean academic styling
plt.rcParams.update({
    'font.family': 'sans-serif',
    'font.sans-serif': ['DejaVu Sans', 'Arial'],
    'font.size': 11,
    'axes.labelsize': 11,
    'axes.titlesize': 12.5,
    'axes.titleweight': 'bold',
    'xtick.labelsize': 10,
    'ytick.labelsize': 10,
    'legend.fontsize': 9.5,
    'axes.spines.top': False,
    'axes.spines.right': False,
    'figure.dpi': 300
})

DATA_DIR = "/home/azhagesan/thesis/Solar-Digital-Twin-Framework/docker/processed_solar_data"
OUTPUT_DIR = "/home/azhagesan/thesis/Solar-Digital-Twin-Framework/eda_figures"
os.makedirs(OUTPUT_DIR, exist_ok=True)

df = pd.read_parquet(DATA_DIR)

# Safely cast plant_id and numeric feature columns
df['plant_id'] = pd.to_numeric(df['plant_id'], errors='coerce').fillna(0).astype(int)
for col in ['ghi_w_m2', 'temp_c', 'actual_power_output_kw', 'power_deviation_ratio']:
    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

# Fix dual timestamp formats (Plant 1: dd-MM-yyyy HH:mm | Plant 2: yyyy-MM-dd HH:mm:ss)
s_clean = df['date_time'].astype(str).str.strip()
ts1 = pd.to_datetime(s_clean, format='%d-%m-%Y %H:%M', errors='coerce')
ts2 = pd.to_datetime(s_clean, format='%Y-%m-%d %H:%M:%S', errors='coerce')
ts3 = pd.to_datetime(s_clean, errors='coerce')
df['timestamp_clean'] = ts1.fillna(ts2).fillna(ts3)
df['hour'] = df['timestamp_clean'].dt.hour

df['Plant Label'] = df['plant_id'].map({
    4135001: 'Plant 1 (Node 0 - Chennai Base)',
    4136001: 'Plant 2 (Node 1 - Kanchipuram Unit)'
})

# Normalize each plant by its own 99th-percentile rated capacity for fair visual comparison
df['power_per_unit'] = df.groupby('plant_id')['actual_power_output_kw'].transform(
    lambda x: np.clip(x / max(x.quantile(0.99), 1e-5), 0, 1)
)
df['ghi_per_unit'] = df.groupby('plant_id')['ghi_w_m2'].transform(
    lambda x: np.clip(x / max(x.quantile(0.99), 1e-5), 0, 1)
)

# =====================================================================
# FIGURE 1: DIURNAL GENERATION CURVE
# =====================================================================
hourly_stats = (
    df.dropna(subset=['hour', 'Plant Label'])
    .groupby(['Plant Label', 'hour'], observed=False)['power_per_unit']
    .agg(['mean', 'std'])
    .reset_index()
)

fig, ax = plt.subplots(figsize=(8.5, 4.6))
colors = {
    'Plant 1 (Node 0 - Chennai Base)': '#1b4f72',
    'Plant 2 (Node 1 - Kanchipuram Unit)': '#d35400'
}

for label, grp in hourly_stats.groupby('Plant Label', observed=False):
    grp = grp.sort_values('hour')
    ax.plot(grp['hour'], grp['mean'], marker='o', markersize=5, linewidth=2.2, label=label, color=colors[label])
    ax.fill_between(
        grp['hour'],
        np.clip(grp['mean'] - grp['std'].fillna(0), 0, 1),
        np.clip(grp['mean'] + grp['std'].fillna(0), 0, 1),
        alpha=0.14, color=colors[label]
    )

ax.set_title('Diurnal Solar Power Generation Profile Across Interconnected Nodes', pad=14)
ax.set_xlabel('Hour of Day (24-Hour Format)', labelpad=8)
ax.set_ylabel('Per-Unit Normalized AC Power Output', labelpad=8)
ax.set_xticks(range(0, 24, 2))
ax.set_ylim(-0.03, 1.05)
ax.grid(True, linestyle='--', alpha=0.45)
ax.legend(title='Digital Twin Node', frameon=True, facecolor='white', framealpha=0.9, loc='upper left')
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_DIR, 'fig1_diurnal_generation_curve.png'), dpi=300)
plt.close()

# =====================================================================
# FIGURE 2: IRRADIANCE VS. POWER CORRELATION
# =====================================================================
sample_df = df.dropna(subset=['Plant Label']).sample(n=min(1500, len(df)), random_state=42)
fig, ax = plt.subplots(figsize=(8.5, 4.6))
scatter_colors = {
    'Plant 1 (Node 0 - Chennai Base)': '#1e8449',
    'Plant 2 (Node 1 - Kanchipuram Unit)': '#c0392b'
}

for label, grp in sample_df.groupby('Plant Label', observed=False):
    ax.scatter(grp['ghi_per_unit'], grp['power_per_unit'], alpha=0.5, s=24, label=label, color=scatter_colors[label], edgecolors='none')

x_vals = np.linspace(0, 1, 100)
ax.plot(x_vals, x_vals, color='#2c3e50', linestyle='--', linewidth=1.8, label='Ideal Clear-Sky Reference')
ax.set_title('Solar Irradiance (GHI) vs. Per-Unit AC Power Correlation', pad=14)
ax.set_xlabel('Per-Unit Normalized Global Horizontal Irradiance (GHI)', labelpad=8)
ax.set_ylabel('Per-Unit Normalized AC Power Output', labelpad=8)
ax.set_xlim(-0.02, 1.03)
ax.set_ylim(-0.02, 1.05)
ax.grid(True, linestyle='--', alpha=0.45)
ax.legend(title='Asset / Physics Reference', frameon=True, facecolor='white', framealpha=0.9, loc='upper left')
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_DIR, 'fig2_ghi_power_correlation.png'), dpi=300)
plt.close()

# =====================================================================
# FIGURE 3: OPERATIONAL STATE & ANOMALY DISTRIBUTION (FIXED HEADROOM)
# =====================================================================
conditions = [
    (df['ghi_per_unit'] <= 0.02),
    (df['power_deviation_ratio'] <= 0.20),
    (df['power_deviation_ratio'] > 0.20) & (df['power_deviation_ratio'] <= 0.85),
    (df['power_deviation_ratio'] > 0.85)
]
categories = [
    'Night / Idle State',
    'Optimal Operation',
    'Moderate Shading / Drift',
    'Severe Anomaly / Fault'
]
df['Operational_State'] = np.select(conditions, categories, default='Optimal Operation')
state_counts = df['Operational_State'].value_counts().reindex(categories).fillna(0)

fig, ax = plt.subplots(figsize=(8.5, 4.6))
bar_colors = ['#7f8c8d', '#27ae60', '#f39c12', '#c0392b']
bars = ax.bar(state_counts.index, state_counts.values, color=bar_colors, edgecolor='#2c3e50', width=0.55)

ax.set_title('Distribution of Operational States and Anomaly Signatures in Curated Dataset', pad=16)
ax.set_xlabel('Operational Classification Category', labelpad=8)
ax.set_ylabel('Number of Telemetry Records', labelpad=8)
ax.set_ylim(0, max(state_counts.values) * 1.16)
ax.grid(axis='y', linestyle='--', alpha=0.45)

for bar in bars:
    yval = bar.get_height()
    ax.text(
        bar.get_x() + bar.get_width() / 2.0,
        yval + (max(state_counts.values) * 0.025),
        f'{int(yval):,}',
        ha='center', va='bottom', fontsize=10.5, fontweight='bold', color='#2c3e50'
    )

plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_DIR, 'fig3_anomaly_state_distribution.png'), dpi=300)
plt.close()
print("✅ Updated publication-grade EDA figures saved to:", OUTPUT_DIR)
