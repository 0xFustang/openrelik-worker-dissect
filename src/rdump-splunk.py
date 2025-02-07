# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import subprocess
import logging
import time 

from typing import Union, List, Dict, Any
from os import environ
from openrelik_worker_common.file_utils import create_output_file
from openrelik_worker_common.task_utils import create_task_result, get_input_files
from .app import celery

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Get Dissect version

dissect_version = subprocess.run(["rdump", "--version"], capture_output=True, text=True)

# Get all Dissect parser names to use for user config form.

protocol_options = ["tcp", "http", "https"]
sourcetype_options = ["records", "json"]

# Task name used to register and route the task to the correct queue.
TASK_NAME = "openrelik-worker-dissect.rdump.splunk"

# Task metadata for registration in the core system.
TASK_METADATA = {
    "display_name": "Dissect: rdump to Splunk",
    "description": "Send Dissect output to Splunk",
    "task_config": [
        {
            "name": "protocol",
            "label": "Protocol to use for forwarding data",
            "description": "Can be tcp, http or https, defaults to tcp if omitted.",
            "type": "autocomplete",
            "items": protocol_options,
            "required": False,
        },
        {
            "name": "sourcetype",
            "label": "Splunk sourcetype",
            "description": "Can be records or json, defaults to records if omitted.",
            "type": "autocomplete",
            "items": sourcetype_options,
            "required": False,
        },
        {
            "name": "token",
            "label": "Splunk HEC token",
            "description": "Authentication token for sending data over HTTP(S)",
            "type": "text",
            "required": False,
        },
        {
            "name": "disable_ssl",
            "label": "Disable SSL verification",
            "description": "Whether to verify the server certificate when sending data over HTTPS",
            "type": "checkbox",
            "required": False,
        },
    ],
}

@celery.task(bind=True, name=TASK_NAME, metadata=TASK_METADATA)
def rdump2splunk(
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

    # Connection details from environment variables

    splunk_host = environ.get("SPLUNK_HOST")
    splunk_port = environ.get("SPLUNK_PORT")

    if not splunk_host or not splunk_port:
        raise RuntimeError("SPLUNK_HOST and SPLUNK_PORT environment variables are required")

    # Handle the parameters

    for key in ["protocol", "sourcetype"]:
        if isinstance(task_config.get(key), list) and len(task_config[key]) > 1:
            raise RuntimeError(f"Select only one {key}, got {task_config[key]}")

    protocols = task_config.get("protocol", "tcp")
    sourcetypes = task_config.get("sourcetype", "records")

    # Determine the protocol
    if isinstance(protocols, list):
        protocols = [p.lower() for p in protocols]
        protocol = protocols[0] if protocols else "tcp"
    else:
        protocol = protocols.lower() if protocols else "tcp"

    logger.info(f"Protocol is {protocol}")

    # Determine the sourcetype
    if isinstance(sourcetypes, list):
        sourcetypes = [st.lower() for st in sourcetypes]
        sourcetype = sourcetypes[0] if sourcetypes else "records"
    else:
        sourcetype = sourcetypes.lower() if sourcetypes else "records"

    logger.info(f"Sourcetype is {sourcetype}")

    if protocol in ["http", "https"] and not task_config.get("token"):
        raise RuntimeError("Splunk HEC token is required for HTTP(S) protocol")
    elif protocol in ["http", "https"]:
        token = task_config.get("token")

    base_command = ["rdump"]

    for input_file in input_files:
        command = base_command + [input_file.get("path")]
        rdump_param = []

        # Parameters
        url = f"splunk+{protocol}://{splunk_host}:{splunk_port}"

        rdump_param.extend(["?sourcetype=", sourcetype])

        if protocol in ["http", "https"]:
            rdump_param.extend(["&token=", token])

        if protocol == "https" and task_config.get("disable_ssl"):
            rdump_param.extend(["&ssl_verify=false"])

        # Join the URL part with parameters
        full_url = f"{url}{''.join(rdump_param)}"
        command.extend(["-w", full_url])

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