import subprocess
import logging
import time 

from os import environ
from openrelik_worker_common.file_utils import create_output_file
from openrelik_worker_common.task_utils import create_task_result, get_input_files
from .app import celery

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Get Dissect version

dissect_version = subprocess.run(["rdump", "--version"], capture_output=True, text=True)

# Task name used to register and route the task to the correct queue.
TASK_NAME = "openrelik-worker-dissect.rdump.jsonl"

# Task metadata for registration in the core system.
TASK_METADATA = {
    "display_name": "Dissect: rdump to JSONL",
    "description": "Create a JSONL file from Dissect",
    "task_config": [],
}

@celery.task(bind=True, name=TASK_NAME, metadata=TASK_METADATA)
def rdump2jsonl(
    self,
    pipe_result: str = None,
    input_files: list = None,
    output_path: str = None,
    workflow_id: str = None,
    task_config: dict = None,
) -> str:
    """Run rdump from Dissect on input files.

    Args:
        pipe_result: Base64-encoded result from the previous Celery task, if any.
        input_files: List of input file dictionaries (unused if pipe_result exists).
        output_path: Path to the output directory.
        workflow_id: ID of the workflow.
        task_config: User configuration for the task.

    Returns:
        Base64-encoded dictionary containing task results.
    """
    input_files = get_input_files(pipe_result, input_files or [])

    # Handle the parameters

    base_command = ["rdump"]

    for input_file in input_files:
        command = base_command + [input_file.get("path")]

        # Parameters

        command.extend(["-J"])

        final_command = " ".join(command)
        logger.info(f"Final command is {final_command}")

        # Run the command
        logger.info("Running rdump")
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()

        if process.returncode == 0:
            logger.info("Rdump finished running successfully")
        else:
            logger.error(f"Rdump failed with return code {process.returncode}")
            logger.error(f"stderr: {stderr.decode('utf-8')}")

        if process.returncode != 0:
            raise RuntimeError(f"Rdump failed with return code {process.returncode}")

    return create_task_result(
        output_files=[],
        workflow_id=workflow_id,
        command=final_command,
        meta={
            "dissect_version": dissect_version.stdout,
        },
    )