import base64
import os
import asyncio
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from traceloop.sdk import Traceloop
from traceloop.sdk.instruments import Instruments

# Parse providers (supports single string, comma-separated, or "all")
raw_providers = [p.strip().lower() for p in provider.split(",")]
if "all" in raw_providers:
    active_providers = {"agentcore", "langfuse", "dynatrace"}
else:
    active_providers = set(raw_providers)

unsupported = active_providers - {"agentcore", "langfuse", "dynatrace"}
if unsupported:
    raise ValueError(f"Unsupported observability provider(s): {unsupported}")

# If ONLY agentcore is selected, opentelemetry-instrument already handles everything
if active_providers == {"agentcore"}:
    return

# Fetch secrets for Langfuse / Dynatrace
obs_secrets = asyncio.run(
    get_combined_app_secrets(
        domain_dict, logger_obj, domain_dict.get("agent_config.appname")
    )
)

# Build OTLP SpanProcessors for each enabled external provider
extra_processors = []

if "langfuse" in active_providers:
    agents_cfg = obs_secrets["global"]["agents"]
    lf_endpoint = f"{agents_cfg['langfuse_host']}/api/public/otel/v1/traces"
    lf_token = base64.b64encode(
        f"{agents_cfg['langfuse_public_key']}:{agents_cfg['langfuse_secret_key']}".encode()
    ).decode()
    lf_exporter = OTLPSpanExporter(
        endpoint=lf_endpoint,
        headers={"Authorization": f"Basic {lf_token}"}
    )
    # Using SimpleSpanProcessor to match your disable_batch=True setting
    extra_processors.append(SimpleSpanProcessor(lf_exporter))

if "dynatrace" in active_providers:
    dynatrace_cfg = obs_secrets["global"]["dynatrace"]
    # Note: Ensure endpoint ends with /v1/traces when passing directly to OTLPSpanExporter
    dt_endpoint = dynatrace_cfg["endpoint"].rstrip("/")
    if not dt_endpoint.endswith("/v1/traces"):
        dt_endpoint = f"{dt_endpoint}/v1/traces"
        
    dt_exporter = OTLPSpanExporter(
        endpoint=dt_endpoint,
        headers={"Authorization": f"Api-Token {dynatrace_cfg['token']}"}
    )
    extra_processors.append(SimpleSpanProcessor(dt_exporter))

# Check if opentelemetry-instrument (ADOT/AgentCore) ALREADY initialized the global TracerProvider
current_provider = trace.get_tracer_provider()

if isinstance(current_provider, TracerProvider):
    # ADOT is already running! Attach Langfuse & Dynatrace processors to the existing provider
    for proc in extra_processors:
        current_provider.add_span_processor(proc)
    
    # If you still need Traceloop's LLM-specific instrumentors (and ADOT didn't load them),
    # initialize Traceloop passing the existing processors, or let ADOT handle instrumentation.
    logger_obj.log_message(
        "INFO",
        f"STATUS=INFO: Attached {len(extra_processors)} extra OTEL processors to existing ADOT TracerProvider for {active_providers}"
    )
else:
    # Plain python3 launch (ADOT disabled) — let Traceloop initialize the TracerProvider with both processors
    Traceloop.init(
        app_name=domain_dict["agent_config"]["agent_name"],
        processor=extra_processors,  # <-- Traceloop accepts a list of SpanProcessors!
        disable_batch=True,
        block_instruments={
            Instruments.REDIS,
            Instruments.REQUESTS,
            Instruments.URLLIB3,
        },
    )
    logger_obj.log_message(
        "INFO",
        f"STATUS=INFO: LAYER=AGENTCORE: Traceloop initialized for providers={active_providers} WORKERID=[{os.getpid()}]"
    )

_patch_openai_stream_accumulator(logger_obj)
