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

from __future__ import annotations

from base64 import b64encode, b64decode
from dataclasses import dataclass, field
from datetime import datetime
import functools
import json
import logging
import operator
from pathlib import PurePath
import re
from typing import Dict, Iterable, Optional, List
import urllib.parse

from kubernetes import client as k8s_client

from pygeoapi.process.manager.kubernetes import KubernetesProcessor

LOGGER = logging.getLogger(__name__)


#: Process metadata and description
PROCESS_METADATA = {
    "version": "0.1.0",
    "id": "execute-notebook",
    "title": "notebooks on kubernetes with papermill",
    "description": "",
    "keywords": ["notebook"],
    "links": [
        {
            "type": "text/html",
            "rel": "canonical",
            "title": "eurodatacube",
            "href": "https://eurodatacube.com",
            "hreflang": "en-US",
        }
    ],
    "inputs": [
        {
            "id": "notebook",
            "title": "notebook file (path relative to home)",
            "abstract": "notebook file",
            "input": {
                "literalDataDomain": {
                    "dataType": "string",
                    "valueDefinition": {"anyValue": True},
                }
            },
            "minOccurs": 1,
            "maxOccurs": 1,
            "metadata": None,  # TODO how to use?
            "keywords": [""],
        },
        {
            "id": "parameters",
            "title": "parameters (base64 encoded yaml)",
            "abstract": "parameters for notebook execution.",
            "input": {
                "literalDataDomain": {
                    "dataType": "string",
                    "valueDefinition": {"anyValue": True},
                }
            },
            "minOccurs": 0,
            "maxOccurs": 1,
            "metadata": None,
            "keywords": [""],
        },
    ],
    "outputs": [
        {
            "id": "result_link",
            "title": "Link to result notebook",
            "description": "Link to result notebook",
            "output": {"formats": [{"mimeType": "text/plain"}]},
        }
    ],
    "example": {},
}


CONTAINER_HOME = PurePath("/home/jovyan")


@dataclass(frozen=True)
class ExtraConfig:
    containers: List[k8s_client.V1Container] = field(default_factory=list)
    volume_mounts: List[k8s_client.V1VolumeMount] = field(default_factory=list)
    volumes: List[k8s_client.V1Volume] = field(default_factory=list)

    def __add__(self, other):
        return ExtraConfig(
            containers=self.containers + other.containers,
            volume_mounts=self.volume_mounts + other.volume_mounts,
            volumes=self.volumes + other.volumes,
        )


class PapermillNotebookKubernetesProcessor(KubernetesProcessor):
    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)
        self.default_image = processor_def["default_image"]
        self.image_pull_secret = processor_def["image_pull_secret"]
        self.s3_bucket_name = processor_def["s3_bucket_name"]
        self.home_volume_claim_name = processor_def["home_volume_claim_name"]
        self.extra_pvcs = processor_def["extra_pvcs"]
        self.jupyer_base_url = processor_def["jupyter_base_url"]

    def create_job_pod_spec(
        self,
        data: Dict,
        job_name: str,
        s3_bucket_config: Optional[KubernetesProcessor.S3BucketConfig],
    ) -> KubernetesProcessor.JobPodSpec:
        LOGGER.debug("Starting job with data %s", data)
        notebook_path = data["notebook"]
        parameters = data.get("parameters", "")
        if not parameters:
            if (parameters_json := data.get("parameters_json")) :
                parameters = b64encode(json.dumps(parameters_json).encode()).decode()

        # TODO: allow override from parameter
        image = self.default_image

        # be a bit smart to select kernel (this should do for now)
        kernel = {
            "eurodatacube/jupyter-user": "edc",
            "eurodatacube/jupyter-user-g": "edc-gpu",
        }.get(image.split(":")[0])
        is_gpu = kernel == "edc-gpu"

        notebook_dir = working_dir(PurePath(notebook_path))

        if (output_notebook := data.get("output_path")) :
            if not PurePath(output_notebook).is_absolute():
                output_notebook = str(notebook_dir / output_notebook)
        else:
            output_notebook = default_output_path(notebook_path)

        extra_podspec = gpu_extra_podspec() if is_gpu else {}

        if self.image_pull_secret:
            extra_podspec["image_pull_secrets"] = [
                k8s_client.V1LocalObjectReference(name=self.image_pull_secret)
            ]

        resources = k8s_client.V1ResourceRequirements(
            limits=drop_none_values(
                {
                    "cpu": data.get("cpu_limit"),
                    "memory": data.get("mem_limit"),
                }
            ),
            requests=drop_none_values(
                {
                    "cpu": data.get("cpu_requests"),
                    "memory": data.get("mem_requests"),
                }
            ),
        )

        def extra_configs() -> Iterable[ExtraConfig]:
            if self.home_volume_claim_name:
                yield home_volume_config(self.home_volume_claim_name)

            yield from (
                extra_pvc_config(extra_pvc={**extra_pvc, "num": num})
                for num, extra_pvc in enumerate(self.extra_pvcs)
            )

            if s3_bucket_config:
                yield s3_config(
                    s3_bucket_config=s3_bucket_config,
                    s3_bucket_name=self.s3_bucket_name,
                )

        extra_config = functools.reduce(operator.add, extra_configs())
        notebook_container = k8s_client.V1Container(
            name="notebook",
            image=image,
            command=[
                "bash",
                # NOTE: we pretend that the shell is interactive such that it
                #       sources /etc/bash.bashrc which activates the default conda
                #       env. This is ok because the default interactive shell must
                #       have PATH set up to include papermill since regular user
                #       should also be able to execute default env commands without extra
                #       setup
                "-i",
                "-c",
                f"papermill "
                f'"{notebook_path}" '
                f'"{output_notebook}" '
                f'--cwd "{notebook_dir}" '
                + (f"-k {kernel} " if kernel else "")
                + (f'-b "{parameters}" ' if parameters else ""),
            ],
            working_dir=str(CONTAINER_HOME),
            volume_mounts=extra_config.volume_mounts,
            resources=resources,
            env=[
                # this is provided in jupyter worker containers and we also use it
                # for compatibility checks
                k8s_client.V1EnvVar(name="JUPYTER_IMAGE", value=image),
                k8s_client.V1EnvVar(name="JOB_NAME", value=job_name),
            ],
        )

        # save parameters but make sure the string is not too long
        extra_annotations = {"parameters": b64decode(parameters).decode()[:8000]}

        return KubernetesProcessor.JobPodSpec(
            pod_spec=k8s_client.V1PodSpec(
                restart_policy="Never",
                # NOTE: first container is used for status check
                containers=[notebook_container] + extra_config.containers,
                volumes=extra_config.volumes,
                # we need this to be able to terminate the sidecar container
                # https://github.com/kubernetes/kubernetes/issues/25908
                share_process_namespace=True,
                service_account="pygeoapi-eoxhub-job",
                **extra_podspec,
            ),
            result={
                "result_type": "link",
                "link": (
                    # NOTE: this link currently doesn't work (even those created in
                    #   the ui with "create sharable link" don't)
                    #   there is a recently closed issue about it:
                    # https://github.com/jupyterlab/jupyterlab/issues/8359
                    #   it doesn't say when it was fixed exactly. there's a possibly
                    #   related fix from last year:
                    # https://github.com/jupyterlab/jupyterlab/pull/6773
                    f"{self.jupyer_base_url}/hub/user-redirect/lab/tree/"
                    + urllib.parse.quote(output_notebook)
                ),
            },
            extra_annotations=extra_annotations,
        )

    def __repr__(self):
        return "<PapermillNotebookKubernetesProcessor> {}".format(self.name)


def default_output_path(notebook_path: str) -> str:
    filename_without_postfix = re.sub(".ipynb$", "", notebook_path)
    now_formatted = datetime.now().strftime("%Y%m%d-%H%M%S-%f")
    return filename_without_postfix + f"_result_{now_formatted}.ipynb"


def working_dir(notebook_path: PurePath) -> PurePath:
    abs_notebook_path = (
        notebook_path
        if notebook_path.is_absolute()
        else (CONTAINER_HOME / notebook_path)
    )
    return abs_notebook_path.parent


def gpu_extra_podspec() -> Dict:
    node_selector = k8s_client.V1NodeSelector(
        node_selector_terms=[
            k8s_client.V1NodeSelectorTerm(
                match_expressions=[
                    k8s_client.V1NodeSelectorRequirement(
                        key="hub.eox.at/node-purpose",
                        operator="In",
                        values=["g2"],
                    ),
                ]
            )
        ]
    )
    return {
        "affinity": k8s_client.V1Affinity(
            node_affinity=k8s_client.V1NodeAffinity(
                required_during_scheduling_ignored_during_execution=node_selector
            )
        ),
        "tolerations": [
            k8s_client.V1Toleration(
                key="hub.eox.at/gpu", operator="Exists", effect="NoSchedule"
            )
        ],
    }


def home_volume_config(home_volume_claim_name: str) -> ExtraConfig:
    return ExtraConfig(
        volumes=[
            k8s_client.V1Volume(
                persistent_volume_claim=k8s_client.V1PersistentVolumeClaimVolumeSource(
                    claim_name=home_volume_claim_name,
                ),
                name="home",
            )
        ],
        volume_mounts=[
            k8s_client.V1VolumeMount(
                mount_path=str(CONTAINER_HOME),
                name="home",
            )
        ],
    )


def extra_pvc_config(extra_pvc: Dict) -> ExtraConfig:
    extra_name = f"extra-{extra_pvc['num']}"
    return ExtraConfig(
        volumes=[
            k8s_client.V1Volume(
                persistent_volume_claim=k8s_client.V1PersistentVolumeClaimVolumeSource(
                    claim_name=extra_pvc["claim_name"],
                ),
                name=extra_name,
            )
        ],
        volume_mounts=[
            k8s_client.V1VolumeMount(
                mount_path=extra_pvc["mount_path"],
                name=extra_name,
            )
        ],
    )


def s3_config(s3_bucket_config, s3_bucket_name) -> ExtraConfig:
    s3_user_bucket_volume_name = "s3-user-bucket"
    return ExtraConfig(
        volume_mounts=[
            k8s_client.V1VolumeMount(
                mount_path=f"{CONTAINER_HOME}/s3",
                name=s3_user_bucket_volume_name,
                mount_propagation="HostToContainer",
            )
        ],
        volumes=[
            k8s_client.V1Volume(
                name=s3_user_bucket_volume_name,
                empty_dir=k8s_client.V1EmptyDirVolumeSource(),
            )
        ],
        containers=[
            k8s_client.V1Container(
                name="s3mounter",
                image="totycro/s3fs:0.4.0-1.86",
                # we need to detect the end of the job here, this container
                # must end for the job to be considered done by k8s
                # 'papermill' is the comm name of the process
                args=[
                    "sh",
                    "-c",
                    'echo "`date` waiting for job start"; '
                    "sleep 5; "
                    'echo "`date` job start assumed"; '
                    "while pgrep -x papermill > /dev/null; do sleep 1; done; "
                    'echo "`date` job end detected"; ',
                ],
                security_context=k8s_client.V1SecurityContext(privileged=True),
                volume_mounts=[
                    k8s_client.V1VolumeMount(
                        name=s3_user_bucket_volume_name,
                        mount_path="/opt/s3fs/bucket",
                        mount_propagation="Bidirectional",
                    ),
                ],
                resources=k8s_client.V1ResourceRequirements(
                    limits={"cpu": "0.1", "memory": "128Mi"},
                    requests={
                        "cpu": "0.05",
                        "memory": "32Mi",
                    },
                ),
                env=[
                    k8s_client.V1EnvVar(name="S3FS_ARGS", value="-oallow_other"),
                    k8s_client.V1EnvVar(name="UID", value="1000"),
                    k8s_client.V1EnvVar(name="GID", value="2014"),
                    k8s_client.V1EnvVar(
                        name="AWS_S3_ACCESS_KEY_ID",
                        value_from=k8s_client.V1EnvVarSource(
                            secret_key_ref=k8s_client.V1SecretKeySelector(
                                name=s3_bucket_config.secret_name,
                                key="username",
                            )
                        ),
                    ),
                    k8s_client.V1EnvVar(
                        name="AWS_S3_SECRET_ACCESS_KEY",
                        value_from=k8s_client.V1EnvVarSource(
                            secret_key_ref=k8s_client.V1SecretKeySelector(
                                name=s3_bucket_config.secret_name,
                                key="password",
                            )
                        ),
                    ),
                    k8s_client.V1EnvVar(
                        "AWS_S3_BUCKET",
                        s3_bucket_name,
                    ),
                    # due to the shared process namespace, tini is not PID 1, so:
                    k8s_client.V1EnvVar(name="TINI_SUBREAPER", value="1"),
                    k8s_client.V1EnvVar(
                        name="AWS_S3_URL",
                        value="https://s3-eu-central-1.amazonaws.com",
                    ),
                ],
            )
        ],
    )


def drop_none_values(d: Dict) -> Dict:
    return {k: v for k, v in d.items() if v is not None}
