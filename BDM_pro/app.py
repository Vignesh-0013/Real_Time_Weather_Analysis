import streamlit as st
from pymongo import MongoClient
import pandas as pd
import time
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

st.title("🌦️ Smart Weather Analytics Dashboard")

# MongoDB
client = MongoClient("mongodb://mongo:27017/")
db = client["weather_db"]
analytics_collection = db["analytics"]
live_collection = db["live_data"]

# 🔧 Graph function
def plot_clean_line(df, column, threshold, title):
    fig, ax = plt.subplots()

    df = df.sort_values("window_end").drop_duplicates(subset=["city"], keep="last")
    df = df.sort_values("city")

    cities = df["city"].tolist()
    values = df[column].tolist()

    ax.plot(cities, values, marker='o')

    for i in range(len(values)):
        if values[i] > threshold:
            ax.plot(cities[i], values[i], 'ro')
        else:
            ax.plot(cities[i], values[i], 'bo')

        ax.text(
            cities[i],
            values[i],
            f"{round(values[i], 1)}",
            ha='center',
            va='bottom'
        )

    ax.set_title(title)
    ax.set_ylabel(column)
    ax.grid(True, linestyle='--', alpha=0.5)

    st.pyplot(fig)

# Placeholders
main_placeholder = st.empty()
table_placeholder = st.empty()
alerts_placeholder = st.empty()

while True:
    data = list(analytics_collection.find({}, {"_id": 0}))

    if data:
        df = pd.DataFrame(data)

        # 🔥 IMPORTANT FIX: convert to datetime
        df["window_end"] = pd.to_datetime(df["window_end"])

        # ------------------ GRAPHS ------------------
        with main_placeholder.container():

            st.subheader("🌡️ Temperature")
            plot_clean_line(df, "avg_temp", 40, "Average Temperature")

            st.subheader("💧 Humidity")
            plot_clean_line(df, "avg_humidity", 80, "Average Humidity")

            st.subheader("🌬️ Wind")
            plot_clean_line(df, "avg_wind", 25, "Average Wind")

            st.subheader("🌧️ Rain")
            plot_clean_line(df, "avg_rain", 15, "Average Rain")

            st.subheader("ضغط Pressure")
            plot_clean_line(df, "avg_pressure", 1035, "Average Pressure")

        # ------------------ LIVE TABLE ------------------
        live_data = list(live_collection.find({}, {"_id": 0}))

        with table_placeholder.container():
            st.subheader("📊 Live Weather Metrics")

            if live_data:
                live_df = pd.DataFrame(live_data)
                live_df = live_df.sort_values("city")

                live_df = live_df.rename(columns={
                    "city": "City",
                    "temperature": "Temperature",
                    "humidity": "Humidity",
                    "wind_speed": "Wind Speed",
                    "rainfall": "Rainfall",
                    "pressure": "Pressure"
                })

                st.dataframe(live_df, width="stretch", height=250)
            else:
                st.warning("No live data yet...")

        # ------------------ ALERTS (FIXED) ------------------
        with alerts_placeholder.container():
            st.subheader("🚨 Active Alerts")

            # Get latest record per city
            latest_df = df.sort_values("window_end").drop_duplicates(
                subset=["city"], keep="last"
            )

            # 🔥 Filter only recent window (last 90 sec)
            now = datetime.utcnow()
            latest_df = latest_df[
                latest_df["window_end"] >= now - timedelta(seconds=90)
            ]

            alerts = []

            for _, row in latest_df.iterrows():
                if row["temp_alert"] == "HIGH":
                    alerts.append(f"{row['city']} → High Temperature")
                if row["humidity_alert"] == "HIGH":
                    alerts.append(f"{row['city']} → High Humidity")
                if row["wind_alert"] == "HIGH":
                    alerts.append(f"{row['city']} → High Wind")
                if row["rain_alert"] == "HIGH":
                    alerts.append(f"{row['city']} → Heavy Rain")
                if row["pressure_alert"] == "HIGH":
                    alerts.append(f"{row['city']} → Abnormal Pressure")

            if alerts:
                for alert in alerts:
                    st.error(alert)
            else:
                st.success("✅ All Normal")

    time.sleep(5)