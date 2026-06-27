from datetime import datetime, timezone
import time
import azure.durable_functions as df
import logging
from pipelineUtils.blob_functions import list_blobs, get_blob_content, write_to_blob
from pipelineUtils import get_month_date
# Libraries used in the future Document Processing client code
from azure.identity import DefaultAzureCredential
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import AnalyzeResult, AnalyzeDocumentRequest
import base64
import json
import os
import requests

from configuration import Configuration
config = Configuration()


name = "runDocIntel"
bp = df.Blueprint()

def normalize_blob_name(container: str, raw_name: str) -> str:
    """Strip container prefix if included in the name."""
    if raw_name.startswith(container + "/"):
        return raw_name[len(container) + 1:]
    return raw_name
def log_docintel_metric(
    blob_name: str,
    container: str,
    stage: str,
    status: str,
    duration_ms: int = 0,
    error_message: str = None,
):
    metric = {
        "activity": name,
        "blob_name": blob_name,
        "container": container,
        "stage": stage,
        "status": status,
        "duration_ms": duration_ms,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "error_message": error_message,
    }

    logging.info(f"docintel_metric={json.dumps(metric)}")
@bp.function_name(name)
@bp.activity_trigger(input_name="blob_input")
def extract_text_from_blob(blob_input: dict):

    start_time = time.perf_counter()

    blob_name = blob_input.get("name")
    container = blob_input.get("container")

    if not blob_name:
        raise ValueError("Missing required blob input field: name")

    if not container:
        raise ValueError("Missing required blob input field: container")

    log_docintel_metric(
        blob_name=blob_name,
        container=container,
        stage="document_intelligence",
        status="started",
    )

    try:
    
        client = DocumentIntelligenceClient(
            endpoint=endpoint, credential=config.credential
        )

        normalized_blob_name = normalize_blob_name(container, blob_name)
        logging.info(f"Normalized Blob Name: {normalized_blob_name}")
        blob_content = get_blob_content(
            container_name=blob_input["container"],
            blob_path=normalized_blob_name
        )

        
        logging.info(f"Starting analyze document: {blob_content[:100]}...")  # Log the first 50 bytes of the file for debugging}")
        model = config.get_value("DOCUMENT_INTELLIGENCE_MODEL")

        if not model:
            model = "prebuilt-read"

        logging.info(f"Using Document Intelligence model: {model}")

        poller = client.begin_analyze_document(
            model,
            AnalyzeDocumentRequest(bytes_source=blob_content)
        )      
        result: AnalyzeResult = poller.result()
        paragraph_count = len(result.paragraphs) if result.paragraphs else 0

        logging.info(
            f"Document analysis completed "
            f"(model={model}, paragraphs={paragraph_count}, blob={normalized_blob_name})"
        )
        if not result.paragraphs:
            raise ValueError(
                f"Document Intelligence returned no paragraph content for blob: {normalized_blob_name}"
            )

        paragraphs = "\n".join([paragraph.content for paragraph in result.paragraphs])

        return paragraphs
        log_docintel_metric(
            blob_name=blob_name,
            container=container,
            stage="document_intelligence",
            status="completed",
            duration_ms=duration_ms,
        )

        return paragraphs
      
    except Exception as e:
        duration_ms = int((time.perf_counter() - start_time) * 1000)

        log_docintel_metric(
            blob_name=blob_name,
            container=container,
            stage="document_intelligence",
            status="failed",
            duration_ms=duration_ms,
            error_message=str(e),
        )

        logging.error(f"Error processing {blob_input}: {e}")
        raise
