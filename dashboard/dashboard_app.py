import streamlit as st
import pandas as pd
import numpy as np
import time

st.set_page_config(page_title="Solar Digital Twin Framework", layout="wide")
st.title("☀️ AI-Powered Interconnected PV Digital Twin Platform")
st.markdown("---")

# =====================================================================
# 1. CORE COORDINATE REGISTRY
# =====================================================================
plant_registry_df = pd.DataFrame({
    'lat': [13.0827, 12.8342],  # Node 0: Chennai Base | Node 1: Kanchipuram Unit
    'lon': [80.2707, 79.7036],
    'Plant ID': ['Asset 4135001 (Node 0)', 'Asset 4136001 (Node 1)'],
    'Location': ['Chennai Central Grid Pooling Node', 'Kanchipuram Regional Generation Site']
})

# =====================================================================
# 2. SCREEN USER INTERFACE SPLIT LAYOUT
# =====================================================================
col_map, col_telemetry = st.columns([1.1, 1])

with col_map:
    st.subheader("🌐 Spatial Correlations Across Neighboring Sites")
    st.map(plant_registry_df, zoom=9)
    
    st.markdown("### 🕸️ Graph Network Topology Verification")
    st.info("Active Spatial Edges: **Node 0 (Chennai) ⇄ Node 1 (Kanchipuram)**")
    
    st.markdown("#### **Active Digital Twin Assets**")
    st.dataframe(plant_registry_df[['Plant ID', 'Location', 'lat', 'lon']], use_container_width=True)

with col_telemetry:
    st.subheader("📊 Near-Real-Time AI Telemetry Feed")
    placeholder = st.empty()
    
    # 3. Near-Real-Time Data Ingestion Stream Loop
    for i in range(50):
        expected_p1 = np.random.uniform(4400, 4550)
        expected_p2 = np.random.uniform(4180, 4320)
        
        # Intermittently trigger anomalies to demonstrate detection capabilities
        if i % 5 == 0:
            actual_p1 = expected_p1 * 0.96
            actual_p2 = expected_p2 * 0.29  
            status_p1 = "✅ Optimal Operation"
            status_p2 = "⚠️ Anomaly Flagged: Localized Grid Failure / Theft Signature"
            alert_active = True
            
            # Weather variables change realistically during an anomaly/cloud cover event
            weather_p1 = {"temp": "36°C", "condition": "☀️ Clear Sky / Sunny", "wind": "12 km/h"}
            weather_p2 = {"temp": "29°C", "condition": "☁️ Heavy Overcast / Cloud Cover", "wind": "22 km/h"}
        else:
            actual_p1 = expected_p1 * 0.95
            actual_p2 = expected_p2 * 0.93
            status_p1 = "✅ Optimal Operation"
            status_p2 = "✅ Optimal Operation"
            alert_active = False
            
            # Normal sunny weather across the local sub-station radius
            weather_p1 = {"temp": "37°C", "condition": "☀️ Clear Sky / Sunny", "wind": "14 km/h"}
            weather_p2 = {"temp": "36°C", "condition": "☀️ Clear Sky / Sunny", "wind": "11 km/h"}
            
        dev_p1 = (expected_p1 - actual_p1) / expected_p1
        dev_p2 = (expected_p2 - actual_p2) / expected_p2
        
        with placeholder.container():
            # ---------------------------------------------------------
            # Telemetry Readout for Plant 1 (Chennai)
            # ---------------------------------------------------------
            st.markdown("#### **Plant Asset 4135001 (Node 0 - Chennai)**")
            
            # Live Weather Sub-Header
            st.markdown(f"🌡️ Temperature: **{weather_p1['temp']}** | ⛅ Sky: **{weather_p1['condition']}** | 💨 Wind: **{weather_p1['wind']}**")
            
            kpi1, kpi2 = st.columns(2)
            kpi1.metric("Expected Capacity", f"{expected_p1:.1f} kW")
            kpi2.metric("Actual Injection", f"{actual_p1:.1f} kW", delta=f"-{dev_p1*100:.1f}%")
            
            inv_eff_p1 = np.random.uniform(97.8, 98.6)
            soil_loss_p1 = dev_p1 * 100 if dev_p1 > 0 else 0.8
            attr1, attr2 = st.columns(2)
            attr1.markdown(f"⚙️ Inverter Efficiency: **{inv_eff_p1:.2f}%**")
            attr2.markdown(f"🍂 Panel Soiling Index: **{soil_loss_p1:.1f}% Loss**")
            st.caption(f"Status Log: {status_p1}")
            
            st.markdown("---")
            
            # ---------------------------------------------------------
            # Telemetry Readout for Plant 2 (Kanchipuram)
            # ---------------------------------------------------------
            st.markdown("#### **Plant Asset 4136001 (Node 1 - Kanchipuram)**")
            
            # Live Weather Sub-Header
            st.markdown(f"🌡️ Temperature: **{weather_p2['temp']}** | ⛅ Sky: **{weather_p2['condition']}** | 💨 Wind: **{weather_p2['wind']}**")
            
            kpi3, kpi4 = st.columns(2)
            kpi3.metric("Expected Capacity", f"{expected_p2:.1f} kW")
            kpi4.metric("Actual Injection", f"{actual_p2:.1f} kW", delta=f"-{dev_p2*100:.1f}%")
            
            inv_eff_p2 = np.random.uniform(97.5, 98.4) if not alert_active else np.random.uniform(45.2, 58.7)
            soil_loss_p2 = dev_p2 * 100 if not alert_active else np.random.uniform(6.5, 9.2)
            attr3, attr4 = st.columns(2)
            attr3.markdown(f"⚙️ Inverter Efficiency: **{inv_eff_p2:.2f}%**")
            attr4.markdown(f"🍂 Panel Soiling Index: **{soil_loss_p2:.1f}% Loss**")
            
            if alert_active:
                st.error(status_p2)
            else:
                st.success(status_p2)
                
            st.line_chart(np.random.normal(4300, 110, size=(15, 2)))
            
        time.sleep(2)  # Refreshes telemetry loops every 2 seconds
