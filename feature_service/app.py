import os
import time
from datetime import datetime, timezone

from fastapi import FastAPI
from pydantic import BaseModel

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor


SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME", "feature-service")
OTEL_EXPORTER_OTLP_ENDPOINT = os.getenv(
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "http://host.docker.internal:4317"
)

resource = Resource.create({
    "service.name": SERVICE_NAME,
    "service.version": "1.0",
    "deployment.environment": "local"
})

trace_provider = TracerProvider(resource=resource)

otlp_exporter = OTLPSpanExporter(
    endpoint=OTEL_EXPORTER_OTLP_ENDPOINT,
    insecure=True
)

trace_provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
trace.set_tracer_provider(trace_provider)

tracer = trace.get_tracer(__name__)

app = FastAPI(
    title="Taxi Feature Service",
    version="1.0",
    description="Serviço auxiliar para enriquecer features antes da inferência"
)

FastAPIInstrumentor.instrument_app(app)


class FeatureRequest(BaseModel):
    day_of_week: int
    is_weekend: int
    month: int
    day: int


@app.get("/")
def root():
    return {
        "service": "feature-service",
        "status": "online",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.post("/enrich")
def enrich_features(request: FeatureRequest):
    with tracer.start_as_current_span("feature_enrichment") as span:
        start = time.time()

        span.set_attribute("feature.day_of_week", request.day_of_week)
        span.set_attribute("feature.is_weekend", request.is_weekend)
        span.set_attribute("feature.month", request.month)
        span.set_attribute("feature.day", request.day)

        # Simula enriquecimento de feature
        seasonal_factor = 1.0

        if request.is_weekend == 1:
            seasonal_factor += 0.08

        if request.month in [11, 12]:
            seasonal_factor += 0.05

        if request.day <= 5:
            seasonal_factor += 0.03

        latency_ms = round((time.time() - start) * 1000, 2)

        span.set_attribute("feature.seasonal_factor", seasonal_factor)
        span.set_attribute("feature.latency_ms", latency_ms)

        return {
            "seasonal_factor": seasonal_factor,
            "feature_latency_ms": latency_ms,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }