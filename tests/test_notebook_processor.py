# =================================================================
#
# Authors: Bernhard Mallinger <bernhard.mallinger@eox.at>
#
# Copyright (C) 2020 EOX IT Services GmbH <https://eox.at>
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

from base64 import b64encode
import copy
import json
from pathlib import Path
import pytest
import stat
from typing import Dict

from pygeoapi.process.notebook import (
    JOB_RUNNER_GROUP_ID,
    PapermillNotebookKubernetesProcessor,
)

OUTPUT_DIRECTORY = "/home/jovyan/foo/test"


@pytest.fixture(autouse=True)
def cleanup_job_directory():
    if (job_dir := Path(OUTPUT_DIRECTORY)).exists():
        for file in job_dir.iterdir():
            file.unlink()


def _create_processor(def_override=None) -> PapermillNotebookKubernetesProcessor:
    return PapermillNotebookKubernetesProcessor(
        processor_def={
            "name": "test",
            "s3": None,
            "default_image": "example",
            "extra_pvcs": [],
            "home_volume_claim_name": "user",
            "image_pull_secret": "",
            "jupyter_base_url": "",
            "output_directory": OUTPUT_DIRECTORY,
            **(def_override if def_override else {}),
        }
    )


@pytest.fixture()
def papermill_processor() -> PapermillNotebookKubernetesProcessor:
    return _create_processor()


@pytest.fixture()
def papermill_gpu_processor() -> PapermillNotebookKubernetesProcessor:
    return _create_processor({"default_image": "eurodatacube/jupyter-user-g:1.2.3"})


@pytest.fixture()
def create_pod_kwargs() -> Dict:
    return {
        "data": {"notebook": "a", "parameters": ""},
        "job_name": "",
    }


def test_workdir_is_notebook_dir(papermill_processor):
    relative_dir = "a/b"
    nb_path = f"{relative_dir}/a.ipynb"
    abs_dir = f"/home/jovyan/{relative_dir}"

    job_pod_spec = papermill_processor.create_job_pod_spec(
        data={"notebook": nb_path, "parameters": ""},
        job_name="",
    )

    assert f'--cwd "{abs_dir}"' in str(job_pod_spec.pod_spec.containers[0].command)


def test_json_params_are_b64_encoded(papermill_processor, create_pod_kwargs):
    payload = {"a": 3}
    create_pod_kwargs = copy.deepcopy(create_pod_kwargs)
    create_pod_kwargs["data"]["parameters_json"] = payload
    job_pod_spec = papermill_processor.create_job_pod_spec(**create_pod_kwargs)

    assert b64encode(json.dumps(payload).encode()).decode() in str(
        job_pod_spec.pod_spec.containers[0].command
    )


def test_custom_output_file_overwrites_default(papermill_processor, create_pod_kwargs):
    output_path = "foo/bar.ipynb"
    create_pod_kwargs = copy.deepcopy(create_pod_kwargs)
    create_pod_kwargs["data"]["output_filename"] = output_path
    job_pod_spec = papermill_processor.create_job_pod_spec(**create_pod_kwargs)

    assert "bar.ipynb" in str(job_pod_spec.pod_spec.containers[0].command)


def test_output_is_written_to_output_dir(create_pod_kwargs):
    output_dir = "/home/jovyan"

    processor = _create_processor({"output_directory": output_dir})
    job_pod_spec = processor.create_job_pod_spec(**create_pod_kwargs)

    assert output_dir + "/a_result" in str(job_pod_spec.pod_spec.containers[0].command)


def test_gpu_image_produces_gpu_kernel(papermill_gpu_processor, create_pod_kwargs):
    job_pod_spec = papermill_gpu_processor.create_job_pod_spec(**create_pod_kwargs)
    assert "-k edc-gpu" in str(job_pod_spec.pod_spec.containers[0].command)


def test_default_image_has_no_affinity(papermill_processor, create_pod_kwargs):
    job_pod_spec = papermill_processor.create_job_pod_spec(**create_pod_kwargs)

    assert job_pod_spec.pod_spec.affinity is None
    assert job_pod_spec.pod_spec.tolerations is None


def test_gpu_image_has_affinity(papermill_gpu_processor, create_pod_kwargs):
    job_pod_spec = papermill_gpu_processor.create_job_pod_spec(**create_pod_kwargs)

    r = (
        job_pod_spec.pod_spec.affinity.node_affinity.required_during_scheduling_ignored_during_execution
    )
    assert r.node_selector_terms[0].match_expressions[0].values == ["g2"]
    assert job_pod_spec.pod_spec.tolerations[0].key == "hub.eox.at/gpu"


def test_no_s3_bucket_by_default(papermill_processor, create_pod_kwargs):
    job_pod_spec = papermill_processor.create_job_pod_spec(**create_pod_kwargs)
    assert "s3mounter" not in [c.name for c in job_pod_spec.pod_spec.containers]
    assert "/home/jovyan/s3" not in [
        m.mount_path for m in job_pod_spec.pod_spec.containers[0].volume_mounts
    ]


def test_s3_bucket_present_when_requested(create_pod_kwargs):
    processor = _create_processor(
        {"s3": {"bucket_name": "example", "secret_name": "example", "s3_url": ""}}
    )
    job_pod_spec = processor.create_job_pod_spec(**create_pod_kwargs)
    assert "s3mounter" in [c.name for c in job_pod_spec.pod_spec.containers]
    assert "/home/jovyan/s3" in [
        m.mount_path for m in job_pod_spec.pod_spec.containers[0].volume_mounts
    ]


def test_extra_pvcs_are_added_on_request(create_pod_kwargs):
    claim_name = "my_pvc"
    processor = _create_processor(
        {"extra_pvcs": [{"claim_name": claim_name, "mount_path": "/mnt"}]}
    )
    job_pod_spec = processor.create_job_pod_spec(**create_pod_kwargs)

    assert claim_name in [
        v.persistent_volume_claim.claim_name
        for v in job_pod_spec.pod_spec.volumes
        if v.persistent_volume_claim
    ]


def test_image_pull_secr_added_when_requested(create_pod_kwargs):
    processor = _create_processor({"image_pull_secret": "psrcr"})
    job_pod_spec = processor.create_job_pod_spec(**create_pod_kwargs)
    assert job_pod_spec.pod_spec.image_pull_secrets[0].name == "psrcr"


def test_output_path_owned_by_job_runner_group_and_group_writable(
    papermill_processor, create_pod_kwargs
):
    output_filename = "foo.ipynb"
    create_pod_kwargs = copy.deepcopy(create_pod_kwargs)
    create_pod_kwargs["data"]["output_filename"] = output_filename
    papermill_processor.create_job_pod_spec(**create_pod_kwargs)

    output_notebook = Path(OUTPUT_DIRECTORY) / output_filename
    assert output_notebook.stat().st_gid == JOB_RUNNER_GROUP_ID

    assert output_notebook.stat().st_mode & stat.S_IWGRP  # write group


def test_custom_kernel_is_used_on_request(papermill_processor, create_pod_kwargs):
    my_kernel = "my-kernel"

    create_pod_kwargs = copy.deepcopy(create_pod_kwargs)
    create_pod_kwargs["data"]["kernel"] = my_kernel
    job_pod_spec = papermill_processor.create_job_pod_spec(**create_pod_kwargs)

    assert f"-k {my_kernel}" in str(job_pod_spec.pod_spec.containers[0].command)
