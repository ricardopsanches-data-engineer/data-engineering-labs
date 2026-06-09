# NYC Taxi Data Platform | Data Engineering, MLOps & Observability

## Overview

This project evolved from a traditional NYC Taxi Data Pipeline into a complete Data Engineering, MLOps, and Observability Platform.

The platform covers the full lifecycle of modern data and machine learning systems, including:

* Data ingestion and transformation
* Cloud data lake and data warehouse integration
* Workflow orchestration
* Machine learning model serving
* Kubernetes deployment
* Model monitoring and drift detection
* Distributed tracing and observability

The objective is to simulate a production-grade platform used by modern data engineering and MLOps teams.

## Platform Components

### Data Engineering

* Kestra
* Google Cloud Storage
* BigQuery
* dbt
* Looker Studio

### MLOps

* FastAPI
* MLflow
* Feature Service
* Model Versioning
* Drift Detection

### Observability

* Prometheus
* Grafana
* Loki
* Tempo
* OpenTelemetry

### Infrastructure

* Docker
* Kubernetes

## Dashboards

### Taxi Platform - Operations & MLOps

Monitoring of:

* API Health
* Request Rate
* Latency
* Prediction Volume
* Prediction Errors
* Drift Percentage
* Kubernetes Resources

### Taxi Platform - End-to-End Observability

Monitoring of:

* Traces per Minute
* Logs per Minute
* Trace Errors
* Correlated Logs
* Distributed Tracing

## Architecture

Data Pipeline

CSV → GCS → BigQuery → dbt → Looker Studio

MLOps Pipeline

Feature Service → FastAPI → MLflow Model

Observability Pipeline

Prometheus → Grafana

Loki → Grafana

Tempo → Grafana

OpenTelemetry → Tempo

## Key Achievements

* End-to-end Data Engineering Pipeline
* Production-style ML Inference API
* Kubernetes Deployment
* Distributed Tracing
* Log-Trace Correlation
* Model Drift Monitoring
* Centralized Observability Stack
* Automated Alerting
