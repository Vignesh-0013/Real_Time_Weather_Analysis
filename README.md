# 🌦️ Real-Time Weather Analysis System

A real-time data streaming and analytics system that simulates weather data, processes it using Apache Spark, stores results in MongoDB, and visualizes insights through an interactive Streamlit dashboard.

---

## 🚀 Project Overview

This project demonstrates a complete real-time data pipeline using modern big data technologies. It continuously generates weather data, processes it in real time, and displays analytics and alerts through a live dashboard.

---

## 🧱 Architecture

```
Producer → Kafka → Spark Streaming → MongoDB → Streamlit Dashboard
```

---

## ⚙️ Tech Stack

* **Apache Kafka** – Real-time data streaming
* **Apache Spark (Structured Streaming)** – Data processing & analytics
* **MongoDB** – NoSQL database for storing results
* **Streamlit** – Interactive dashboard
* **Docker & Docker Compose** – Containerization

---

## 🔄 Workflow

1. **Data Generation**

   * Simulated weather data (temperature, humidity, wind, rainfall, pressure)
   * Sent to Kafka topic every few seconds

2. **Streaming (Kafka)**

   * Acts as a buffer and message broker
   * Stores incoming real-time data

3. **Processing (Spark)**

   * Reads data from Kafka
   * Applies 60-second window aggregation
   * Computes:

     * Average values
     * Min/Max temperature
   * Generates alerts based on thresholds

4. **Storage (MongoDB)**

   * `analytics` → aggregated results
   * `live_data` → latest weather values

5. **Visualization (Streamlit)**

   * Graphs for analytics
   * Live table for current data
   * Alerts for abnormal conditions

---

## 📊 Features

* 📡 Real-time data streaming
* 📈 Live updating dashboard
* 🚨 Alert system for abnormal conditions
* 🧠 Window-based analytics (last 60 seconds)
* 🐳 Fully containerized using Docker

---

## 📁 Project Structure

```
weather-project/
│── docker-compose.yml
│── producer.py
│── spark_job.py
│── app.py
│── requirements.txt
```

---

## ▶️ How to Run

### 1. Clone the repository

```bash
git clone https://github.com/Vignesh-0013/Real_Time_Weather_Analysis.git
cd Real_Time_Weather_Analysis
```

### 2. Start services

```bash
docker-compose up --build
```

### 3. Open dashboard

```
http://localhost:8501
```

---

## ⚠️ Notes

* Ensure Kafka topic `weather-topic` is created before running Spark
* MongoDB runs inside Docker container
* Dashboard refreshes every 5 seconds

---

## 📸 Output

* Live weather data table
* Graphical analytics
* Real-time alerts

---

## 🎯 Learning Outcomes

* Understanding real-time streaming systems
* Working with Kafka and Spark integration
* Implementing window-based analytics
* Building full-stack data pipelines
* Deploying applications using Docker

---

## 👨‍💻 Author

**Vignesh S**
GitHub: https://github.com/Vignesh-0013

---

## ⭐ Acknowledgement

This project is developed as part of academic learning to demonstrate real-time big data processing concepts.

---
