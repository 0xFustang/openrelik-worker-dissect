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

from dissect.target.target import Target
from dissect.target.plugin import find_plugin_functions

from openrelik_worker_common.file_utils import create_output_file
from openrelik_worker_common.task_utils import create_task_result, get_input_files
from .app import celery

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Get Dissect version

dissect_version = subprocess.run(["target-query", "--version"], capture_output=True, text=True)

# Get all Dissect parser names to use for user config form.

collected_plugins = []

funcs, _ = find_plugin_functions(Target(), "*", compatibility=False, show_hidden=True)
for func in funcs:
    collected_plugins.append(str(func))

# Task name used to register and route the task to the correct queue.
TASK_NAME = "openrelik-worker-dissect.target-query"

# Task metadata for registration in the core system.
TASK_METADATA = {
    "display_name": "Dissect: target-query",
    "description": "Timelining using Dissect",
    # Configuration that will be rendered as a web for in the UI, and any data entered
    # by the user will be available to the task function when executing (task_config).
    "task_config": [
        {
            "name": "plugins",
            "label": "Select plugins to use",
            "description": "Select one or more Dissect parsers to use. If none are selected, all will be used.",
            "type": "autocomplete",
            "items": collected_plugins,
            "required": False,
        },
    ],
}

@celery.task(bind=True, name=TASK_NAME, metadata=TASK_METADATA)
def dissect(
    self,
    pipe_result: str = None,
    input_files: list = None,
    output_path: str = None,
    workflow_id: str = None,
    task_config: dict = None,
) -> str:
    """Run target-query from Dissect on input files.

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
    output_files = []
    base_command = ["target-query"]

    for input_file in input_files:
        output_file = create_output_file(
            output_path,
            extension="dump",
            data_type="dissect:target:dump_file",
        )

        command = base_command + [input_file.get("path")]

        plugins = task_config.get("plugins", collected_plugins) if task_config else collected_plugins
        if not isinstance(plugins, (list, tuple)):
            plugins = collected_plugins
        command.extend(["-f", ",".join(plugins)])

        command.extend(["-q", ">", output_file.path])

        final_command = " ".join(command)

        # Run the command
        with open(output_file.path, "w") as fh:
            logger.info("Running target-query")
            process = subprocess.Popen(command, stdout=fh)
            while process.poll() is None:
                time.sleep(1)
        
        logger.info("target-query finished running")

        if process.returncode != 0:
            raise RuntimeError(f"target-query failed with return code {process.returncode}")

        output_files.append(output_file.to_dict())
    
    # Dissect parsing

    if not output_files:
        raise RuntimeError("Dissect didn't create any output files")
    else:
        logger.info(f"Dissect created {len(output_files)} output files")

    return create_task_result(
        output_files=output_files,
        workflow_id=workflow_id,
        command=final_command,
        meta={
            "dissect_version": dissect_version.stdout,
        },
    )
