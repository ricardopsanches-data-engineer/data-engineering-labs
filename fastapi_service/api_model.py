import os
import time
import logging
from datetime import datetime, timezone

import mlflow.pyfunc
import pandas as pd
import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pythonjsonlogger import jsonlogger
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_client import Counter, Gauge, Histogram

from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor


# ==========================================
# OPENTELEMETRY CONFIG
# ==========================================

SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME", "taxi-fastapi")

OTEL_EXPORTER_OTLP_ENDPOINT = os.getenv(
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "http://host.docker.internal:4317"
)

FEATURE_SERVICE_URL = os.getenv(
    "FEATURE_SERVICE_URL",
    "http://host.docker.internal:8090/enrich"
)

resource = Resource.create(
    {
        "service.name": SERVICE_NAME,
        "service.version": "2.9",
        "deployment.environment": "docker-local",
    }
)

trace_provider = TracerProvider(resource=resource)

otlp_exporter = OTLPSpanExporter(
    endpoint=OTEL_EXPORTER_OTLP_ENDPOINT,
    insecure=True,
)

trace_provider.add_span_processor(
    BatchSpanProcessor(otlp_exporter)
)

trace.set_tracer_provider(trace_provider)
tracer = trace.get_tracer("taxi-fastapi")


# Instrument outgoing HTTP calls made with requests.
# This is what helps Tempo/Grafana understand that taxi-fastapi calls feature-service.
try:
    RequestsInstrumentor().instrument()
except Exception:
    pass


# ==========================================
# JSON LOGGER
# ==========================================

class TraceContextFilter(logging.Filter):
    def filter(self, record):
        span = trace.get_current_span()
        context = span.get_span_context()

        if context and context.is_valid:
            record.trace_id = format(context.trace_id, "032x")
            record.span_id = format(context.span_id, "016x")
        else:
            record.trace_id = None
            record.span_id = None

        return True


logger = logging.getLogger("taxi_api")
logger.setLevel(logging.INFO)
logger.handlers.clear()

log_handler = logging.StreamHandler()

formatter = jsonlogger.JsonFormatter(
    "%(asctime)s %(levelname)s %(message)s "
    "%(endpoint)s %(prediction)s %(drift_percent)s "
    "%(latency_ms)s %(model_version)s %(status)s "
    "%(trace_id)s %(span_id)s %(feature_service_status)s "
    "%(seasonal_factor)s"
)

log_handler.setFormatter(formatter)
log_handler.addFilter(TraceContextFilter())
logger.addHandler(log_handler)


# ==========================================
# CONFIG
# ==========================================

MODEL_URI = os.getenv("MODEL_URI", "./model_artifact")
MODEL_VERSION = os.getenv("MODEL_VERSION", "v20260526")


# ==========================================
# CUSTOM PROMETHEUS METRICS
# ==========================================

PREDICTION_VALUE = Gauge(
    "taxi_prediction_value",
    "Ultimo valor previsto pelo modelo",
    ["model_version"],
)

DRIFT_PERCENT = Gauge(
    "taxi_drift_percent",
    "Ultimo drift percentual calculado",
    ["model_version"],
)

INFERENCE_COUNTER = Counter(
    "taxi_inference_requests_total",
    "Total de inferencias realizadas",
    ["model_version", "status"],
)

INFERENCE_LATENCY = Histogram(
    "taxi_inference_latency_ms",
    "Latencia da inferencia em milissegundos",
    ["model_version"],
    buckets=[10, 25, 50, 75, 100, 250, 500, 1000, 2500],
)


# ==========================================
# FASTAPI APP
# ==========================================

app = FastAPI(
    title="Taxi Trips ML API",
    version="2.9",
    description=(
        "API de predicao de viagens de taxi com logs estruturados, "
        "metricas Prometheus, metricas customizadas, OpenTelemetry tracing "
        "e chamada distribuida para feature-service"
    ),
)

# Prometheus metrics endpoint.
Instrumentator().instrument(app).expose(app)

# OpenTelemetry FastAPI instrumentation.
# Important: exclude /metrics so Prometheus scraping does not pollute Tempo.
FastAPIInstrumentor.instrument_app(
    app,
    excluded_urls="/metrics",
)


# ==========================================
# LOAD MODEL
# ==========================================

try:
    model = mlflow.pyfunc.load_model(MODEL_URI)

    logger.info(
        "model_loaded",
        extra={
            "endpoint": "startup",
            "prediction": None,
            "drift_percent": None,
            "latency_ms": None,
            "model_version": MODEL_VERSION,
            "status": 200,
            "feature_service_status": None,
            "seasonal_factor": None,
        },
    )

except Exception as e:
    logger.error(
        "model_load_error",
        extra={
            "endpoint": "startup",
            "prediction": None,
            "drift_percent": None,
            "latency_ms": None,
            "model_version": MODEL_VERSION,
            "status": 500,
            "feature_service_status": None,
            "seasonal_factor": None,
            "error": str(e),
        },
    )
    raise e


# ==========================================
# REQUEST SCHEMA
# ==========================================

class PredictionRequest(BaseModel):
    day_of_week: int
    is_weekend: int
    month: int
    day: int
    total_passengers: float
    avg_trip_distance: float
    avg_fare_amount: float
    lag_1_total_trips: int
    lag_7_total_trips: int
    rolling_avg_7_days: float
    rolling_avg_14_days: float


# ==========================================
# ROUTES
# ==========================================

@app.get("/")
def root():
    return {
        "message": "Taxi Trips ML API",
        "status": "online",
        "deployment": "docker-local",
        "api_version": "2.9",
        "model_uri": MODEL_URI,
        "model_version": MODEL_VERSION,
        "metrics_endpoint": "/metrics",
        "tracing": "enabled",
        "otel_service_name": SERVICE_NAME,
        "otel_endpoint": OTEL_EXPORTER_OTLP_ENDPOINT,
        "feature_service_url": FEATURE_SERVICE_URL,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.post("/predict")
def predict(request: PredictionRequest):
    start_time = time.time()

    # Span principal com nome facil de buscar no Grafana/Tempo.
    with tracer.start_as_current_span("POST /predict") as span:
        try:
            span.set_attribute("app.endpoint", "/predict")
            span.set_attribute("app.operation", "model_prediction")
            span.set_attribute("model.version", MODEL_VERSION)

            span.set_attribute("input.day_of_week", request.day_of_week)
            span.set_attribute("input.is_weekend", request.is_weekend)
            span.set_attribute("input.month", request.month)
            span.set_attribute("input.day", request.day)

            # ==========================================
            # CALL FEATURE SERVICE
            # ==========================================

            with tracer.start_as_current_span("POST feature-service /enrich") as feature_span:
                feature_payload = {
                    "day_of_week": request.day_of_week,
                    "is_weekend": request.is_weekend,
                    "month": request.month,
                    "day": request.day,
                }

                feature_span.set_attribute("feature.service.url", FEATURE_SERVICE_URL)
                feature_span.set_attribute("http.method", "POST")
                feature_span.set_attribute("http.url", FEATURE_SERVICE_URL)

                feature_response = requests.post(
                    FEATURE_SERVICE_URL,
                    json=feature_payload,
                    timeout=5,
                )

                feature_response.raise_for_status()
                feature_data = feature_response.json()

                seasonal_factor = float(feature_data.get("seasonal_factor", 1.0))
                feature_latency_ms = float(feature_data.get("feature_latency_ms", 0.0))

                feature_span.set_attribute("feature.seasonal_factor", seasonal_factor)
                feature_span.set_attribute("feature.latency_ms", feature_latency_ms)
                feature_span.set_attribute("http.status_code", feature_response.status_code)
                feature_span.set_status(Status(StatusCode.OK))

            span.set_attribute("feature.seasonal_factor", seasonal_factor)
            span.set_attribute("feature.latency_ms", feature_latency_ms)

            # ==========================================
            # MODEL INPUT
            # ==========================================

            input_data = pd.DataFrame(
                {
                    "day_of_week": pd.Series([request.day_of_week], dtype="int64"),
                    "is_weekend": pd.Series([request.is_weekend], dtype="int64"),
                    "month": pd.Series([request.month], dtype="int64"),
                    "day": pd.Series([request.day], dtype="int64"),
                    "total_passengers": pd.Series([request.total_passengers], dtype="float64"),
                    "avg_trip_distance": pd.Series([request.avg_trip_distance], dtype="float64"),
                    "avg_fare_amount": pd.Series([request.avg_fare_amount], dtype="float64"),
                    "lag_1_total_trips": pd.Series([request.lag_1_total_trips], dtype="int64"),
                    "lag_7_total_trips": pd.Series([request.lag_7_total_trips], dtype="int64"),
                    "rolling_avg_7_days": pd.Series([request.rolling_avg_7_days], dtype="float64"),
                    "rolling_avg_14_days": pd.Series([request.rolling_avg_14_days], dtype="float64"),
                }
            )

            # ==========================================
            # MODEL PREDICTION
            # ==========================================

            with tracer.start_as_current_span("mlflow_model_predict") as model_span:
                model_span.set_attribute("model.uri", MODEL_URI)
                model_span.set_attribute("model.version", MODEL_VERSION)

                prediction = model.predict(input_data)[0]

                model_span.set_attribute("prediction.raw_value", float(prediction))
                model_span.set_status(Status(StatusCode.OK))

            prediction = float(prediction) * seasonal_factor

            historical_average = request.rolling_avg_14_days

            if historical_average and historical_average != 0:
                drift_percent = round(
                    ((float(prediction) - historical_average) / historical_average) * 100,
                    4,
                )
            else:
                drift_percent = 0.0

            latency_ms = round((time.time() - start_time) * 1000, 2)

            span.set_attribute("prediction.value", float(prediction))
            span.set_attribute("prediction.drift_percent", drift_percent)
            span.set_attribute("prediction.latency_ms", latency_ms)
            span.set_attribute("http.status_code", 200)
            span.set_status(Status(StatusCode.OK))

            # ==========================================
            # PROMETHEUS CUSTOM METRICS
            # ==========================================

            PREDICTION_VALUE.labels(
                model_version=MODEL_VERSION,
            ).set(float(prediction))

            DRIFT_PERCENT.labels(
                model_version=MODEL_VERSION,
            ).set(drift_percent)

            INFERENCE_COUNTER.labels(
                model_version=MODEL_VERSION,
                status="success",
            ).inc()

            INFERENCE_LATENCY.labels(
                model_version=MODEL_VERSION,
            ).observe(latency_ms)

            logger.info(
                "prediction_request",
                extra={
                    "endpoint": "/predict",
                    "prediction": round(float(prediction), 4),
                    "drift_percent": drift_percent,
                    "latency_ms": latency_ms,
                    "model_version": MODEL_VERSION,
                    "status": 200,
                    "feature_service_status": 200,
                    "seasonal_factor": seasonal_factor,
                },
            )

            trace_id = format(span.get_span_context().trace_id, "032x")
            span_id = format(span.get_span_context().span_id, "016x")

            # Force flush ajuda muito no ambiente local.
            trace_provider.force_flush()

            return {
                "prediction": round(float(prediction), 3),
                "drift_percent": drift_percent,
                "model_uri": MODEL_URI,
                "model_version": MODEL_VERSION,
                "latency_ms": latency_ms,
                "seasonal_factor": seasonal_factor,
                "feature_service_latency_ms": feature_latency_ms,
                "trace_id": trace_id,
                "span_id": span_id,
                "prediction_timestamp": datetime.now(timezone.utc).isoformat(),
            }

        except Exception as e:
            latency_ms = round((time.time() - start_time) * 1000, 2)

            span.record_exception(e)
            span.set_attribute("http.status_code", 500)
            span.set_attribute("error", True)
            span.set_attribute("error.message", str(e))
            span.set_status(Status(StatusCode.ERROR, str(e)))

            INFERENCE_COUNTER.labels(
                model_version=MODEL_VERSION,
                status="error",
            ).inc()

            logger.error(
                "prediction_error",
                extra={
                    "endpoint": "/predict",
                    "prediction": None,
                    "drift_percent": None,
                    "latency_ms": latency_ms,
                    "model_version": MODEL_VERSION,
                    "status": 500,
                    "feature_service_status": "error",
                    "seasonal_factor": None,
                    "error": str(e),
                },
            )

            trace_provider.force_flush()

            raise HTTPException(status_code=500, detail=str(e))