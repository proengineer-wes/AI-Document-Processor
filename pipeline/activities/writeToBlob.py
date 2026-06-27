import azure.durable_functions as df
import logging
from datetime import datetime, timezone
import time
from pipelineUtils.blob_functions import write_to_blob
import os
import json

from configuration import Configuration
config = Configuration()

FINAL_OUTPUT_CONTAINER = config.get_value("FINAL_OUTPUT_CONTAINER")

logging.info(f"writeToBlob.py: FINAL_OUTPUT_CONTAINER is {FINAL_OUTPUT_CONTAINER}")

name = "writeToBlob"
bp = df.Blueprint()


def log_upload_metadata(
    blob_name: str,
    output_blob: str,
    container: str,
    payload_size_bytes: int,
    duration_ms: int,
    status: str,
    error_message: str = None,
):
    metadata = {
        "activity": name,
        "source_blob": blob_name,
        "output_blob": output_blob,
        "container": container,
        "payload_size_bytes": payload_size_bytes,
        "duration_ms": duration_ms,
        "status": status,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "error_message": error_message,
    }

    logging.info(f"blob_upload_metadata={json.dumps(metadata)}")


@bp.function_name(name)
@bp.activity_trigger(input_name="args")
def write_to_blob_activity(args: dict):
    """
    Writes the JSON bytes to a blob storage.
    Args:
        args (dict): A dictionary containing the blob name and JSON bytes.
    """
    start_time = time.perf_counter()
    blob_name = None
    output_blob = None
    final_output_container = None
    payload_size_bytes = 0

    try:
        blob_name = args.get("blob_name")
        final_output_container = args.get("final_output_container")
        json_str = args.get("json_str")

        if not blob_name:
            raise ValueError("Missing required argument: blob_name")

        if not final_output_container:
            raise ValueError("Missing required argument: final_output_container")

        if json_str is None:
            raise ValueError("Missing required argument: json_str")

        json_bytes = json_str.encode("utf-8")
        payload_size_bytes = len(json_bytes)

        sourcefile = os.path.splitext(os.path.basename(blob_name))[0]
        output_blob = f"{sourcefile}-output.json"

        logging.info(
            f"writeToBlob.py: Writing output to blob {output_blob} "
            f"with source file {sourcefile} and FINAL_OUTPUT_CONTAINER {final_output_container}"
        )

        result = write_to_blob(final_output_container, output_blob, json_bytes)

        duration_ms = int((time.perf_counter() - start_time) * 1000)

        log_upload_metadata(
            blob_name=blob_name,
            output_blob=output_blob,
            container=final_output_container,
            payload_size_bytes=payload_size_bytes,
            duration_ms=duration_ms,
            status="completed" if result else "failed",
        )

        logging.info(f"writeToBlob.py: Result of write_to_blob: {result}")

        if result:
            logging.info(f"writeToBlob.py: Successfully wrote output to blob {blob_name}")
            return {
                "success": True,
                "blob_name": blob_name,
                "output_blob": output_blob,
            }

        logging.error(f"Failed to write output to blob {blob_name}")
        return {
            "success": False,
            "error": "Failed to write output",
        }

    except Exception as e:
        duration_ms = int((time.perf_counter() - start_time) * 1000)

        log_upload_metadata(
            blob_name=blob_name or "unknown",
            output_blob=output_blob or "unknown",
            container=final_output_container or "unknown",
            payload_size_bytes=payload_size_bytes,
            duration_ms=duration_ms,
            status="failed",
            error_message=str(e),
        )

        error_msg = f"Error writing output for blob {blob_name}: {str(e)}"
        logging.error(error_msg)
        raise