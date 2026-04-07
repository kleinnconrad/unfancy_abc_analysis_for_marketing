# Unfancified Marketing Analytics Pipeline

[![Deploy Databricks Asset Bundle](https://github.com/YOUR_USERNAME/YOUR_REPO/actions/workflows/deploy.yml/badge.svg)](https://github.com/YOUR_USERNAME/YOUR_REPO/actions/workflows/deploy.yml)

A lightweight, transparent Databricks data pipeline that segments customers and predicts VIP potential without relying on black-box machine learning models. 

This repository demonstrates how to build a complete Descriptive-to-Predictive analytics workflow using Databricks Asset Bundles (DABs), Unity Catalog Volumes, and Delta Tables, relying entirely on robust, hardcoded heuristics rather than complex algorithms.

## Table of Contents
- [The "Unfancified" Philosophy](#the-unfancified-philosophy)
- [Pipeline Architecture (The DAG)](#pipeline-architecture-the-dag)
  - [1. Ingestion](#1-ingestion-00_generate_mock_datapy)
  - [2. Descriptive Analytics](#2-descriptive-analytics-01_abc_analysispy)
  - [3. Diagnostic Analytics](#3-diagnostic-analytics-02_characterizationpy)
  - [4. Predictive Analytics](#4-predictive-analytics-03_predictionpy)
- [Deployment & CI/CD](#deployment--cicd)
- [Why This Matters](#why-this-matters)

## The "Unfancified" Philosophy

Modern data ecosystems often default to machine learning for any predictive task. However, outputting a probability score (e.g., "78% chance to become a VIP") is often useless to downstream marketing teams running legacy CRM software that only understands basic boolean logic.

This pipeline "unfancifies" the standard data science workflow:

| Feature | The Common Approach (Fancified) | The Unfancified Approach |
| :--- | :--- | :--- |
| **Segmentation** | K-Means clustering or complex CRM scoring modules. | **ABC Analysis** using the Pareto principle (80/20 rule) based on 12-month historical revenue. |
| **Characterization** | SHAP values, feature importance, and correlation matrices. | **A/B Averages**. Grouping early telemetry by ABC class to find the "Magic Metric" gaps. |
| **Prediction** | Logistic regression or Random Forest models deployed as API endpoints. | **Heuristic Thresholds**. Simple `IF A AND B THEN True` logic applied directly in PySpark. |
| **Output** | Opaque probabilistic scores. | Fully transparent, auditable categorical flags written to a Delta Table. |

By relying on descriptive math to generate predictive rules, this pipeline achieves near-instant deployment, zero model-drift maintenance, and 100% transparency for business stakeholders.

## Pipeline Architecture (The DAG)

The workflow is orchestrated as a Directed Acyclic Graph (DAG) using Databricks Jobs. Because tasks in a DAG run on distributed clusters, they cannot pass data directly in memory. Instead, this pipeline uses a **Databricks Unity Catalog Volume** (`/Volumes/workspace/default/data/`) to pass state between steps.

The pipeline consists of four sequential Python tasks:

### 1. Ingestion: `00_generate_mock_data.py`
Simulates the data engineering layer. It generates two synthetic datasets: historical customers (with 12-month spend data) and a fresh cohort of new sign-ups (with only 14 days of engagement data). It writes these as CSVs to the Unity Catalog Volume.

### 2. Descriptive Analytics: `01_abc_analysis.py`
Reads the historical dataset and calculates the cumulative revenue percentage for every user. It segments the user base into `A_VIP` (top 80% of revenue), `B_Core` (next 15%), and `C_LongTail` (bottom 5%). It saves the labeled dataset back to the Volume.

### 3. Diagnostic Analytics: `02_characterization.py`
Reads the labeled historical data and calculates the average early engagement (content consumed, login days) for each segment. It identifies the behavioral gap between VIPs and average users, extracting a "Magic Metric" threshold (e.g., > 10 content views AND > 5 logins). These rules are saved as a lightweight `magic_thresholds.json` file.

### 4. Predictive Analytics: `03_prediction.py`
The operational final mile. It reads the fresh, unlabeled cohort data and the dynamic JSON thresholds. It applies the flat boolean logic using `numpy/pandas` and uses PySpark to write a comprehensive, auditable **Delta Table** (`workspace.default.marketing_cohort_predictions`). This table contains the predicted marketing segment alongside the specific thresholds that triggered the decision, ready for immediate CRM ingestion.

## Deployment & CI/CD

This project uses **Databricks Asset Bundles (DABs)** to manage infrastructure as code. 

* **`databricks.yml`**: Defines the job cluster (`i3.xlarge`), wires the four Python scripts together sequentially, and handles the deployment configuration to the target Databricks workspace.
* **GitHub Actions (`.github/workflows/deploy.yml`)**: Automates the deployment. Every push to the `main` branch triggers the Databricks CLI to authenticate (via a repository secret) and deploy the bundle to the production workspace.

### Prerequisites for Deployment
1. A Databricks Workspace with Unity Catalog enabled.
2. A Unity Catalog Volume created at `/Volumes/workspace/default/data/`.
3. A Databricks Personal Access Token (PAT) or Service Principal token saved as a GitHub Action Secret named `DATABRICKS_TOKEN`.

## Why This Matters

Sometimes the best solution isn't a complex microservice—it's a mathematically sound heuristic running natively on your data warehouse. This repository provides a scalable template for data teams looking to deliver immediate business value without the overhead of MLOps.
