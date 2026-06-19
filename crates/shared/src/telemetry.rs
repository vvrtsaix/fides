//! Tracing init. JSON/pretty stdout always; OTLP span export to a collector (Jaeger) when
//! `otel_endpoint` is set. Spans are produced by the HTTP `TraceLayer` and `#[tracing::instrument]`
//! on hot paths, then exported over OTLP/HTTP.

use opentelemetry::trace::TracerProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{runtime, trace::TracerProvider, Resource};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

pub fn init(log_format: &str, otel_endpoint: Option<&str>, service_name: &str) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new(
            "info,fides_api=debug,fides_worker=debug,fides_db=debug,tower_http::trace=debug",
        )
    });

    let fmt_layer = if log_format == "pretty" {
        tracing_subscriber::fmt::layer().pretty().boxed()
    } else {
        tracing_subscriber::fmt::layer().json().boxed()
    };

    let otel_layer = otel_endpoint.map(|endpoint| {
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_endpoint(format!("{}/v1/traces", endpoint.trim_end_matches('/')))
            .build()
            .expect("build OTLP span exporter");
        let provider = TracerProvider::builder()
            .with_batch_exporter(exporter, runtime::Tokio)
            .with_resource(Resource::new(vec![KeyValue::new(
                "service.name",
                service_name.to_string(),
            )]))
            .build();
        let tracer = provider.tracer("fides");
        opentelemetry::global::set_tracer_provider(provider);
        tracing_opentelemetry::layer().with_tracer(tracer)
    });

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .with(otel_layer)
        .init();
}
