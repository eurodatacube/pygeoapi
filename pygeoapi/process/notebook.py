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

from pygeoapi.process.manager.kubernetes import (
    KubernetesProcessor,
    format_annotation_key,
)

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

        # TODO: config file parsing?
        self.default_image: str = processor_def["default_image"]
        self.image_pull_secret: str = processor_def["image_pull_secret"]
        self.s3: Dict[str, str] = processor_def.get("s3")
        self.home_volume_claim_name: str = processor_def["home_volume_claim_name"]
        self.extra_pvcs: List = processor_def["extra_pvcs"]
        self.jupyer_base_url: str = processor_def["jupyter_base_url"]

    def create_job_pod_spec(
        self,
        data: Dict,
        job_name: str,
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

            if self.s3:
                yield s3_config(
                    bucket_name=self.s3["bucket_name"],
                    secret_name=self.s3["secret_name"],
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
                f"{install_papermill_kubernetes_job_progress} && "
                f"papermill "
                f'"{notebook_path}" '
                f'"{output_notebook}" '
                "--engine kubernetes_job_progress "
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
                k8s_client.V1EnvVar(
                    name="PROGRESS_ANNOTATION", value=format_annotation_key("progress")
                ),
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


def s3_config(bucket_name, secret_name) -> ExtraConfig:
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
                                name=secret_name,
                                key="username",
                            )
                        ),
                    ),
                    k8s_client.V1EnvVar(
                        name="AWS_S3_SECRET_ACCESS_KEY",
                        value_from=k8s_client.V1EnvVarSource(
                            secret_key_ref=k8s_client.V1SecretKeySelector(
                                name=secret_name,
                                key="password",
                            )
                        ),
                    ),
                    k8s_client.V1EnvVar(
                        "AWS_S3_BUCKET",
                        bucket_name,
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


# NOTE: this is a "temporary" way of getting this installed, we should place this
#       in the base image in the next iteration.

# generated using:
# python3 -m pip  wheel --no-deps -w wheels . &&
#     base64 < wheels/papermill_kubernetes_job_progress-0.1-py3-none-any.whl
papermill_kubernetes_job_progress_wheel_name = (
    "papermill_kubernetes_job_progress-0.1-py3-none-any.whl"
)
papermill_kubernetes_job_progress_wheel_base64 = """UEsDBBQAAAAIAIFJd1HwaB6gUAIAALkFAAAtAAAAcGFwZXJtaWxsX2t1YmVybmV0ZXNfam9iX3By
b2dyZXNzL19faW5pdF9fLnB5fVRNb9swDL37VxDZoU6W2uhtCDBg6VAM67CkaItehsGQZcbRIkuG
KGcFhv730U781aTVySLfo8hHmqoorfNgKQg2zhawq1J0Bj0SqINLaoXGgyDYfaLkcJuDtGaj8s7a
3A4RSlGiK5TWEZpcmT7Q6vprQ75pzEEQSC2I4Ef34q1N75zNHRIdMOGYMl0EwOdLwyvQb23WGDLc
AD6jrDwmhTAixywx1mNq7S6UmuZg0toxh5lwOV9ns93f+osDNgHq05cRaSuyRBmpK/LojtZw2kFT
4eU22V/B54Ek0XVtfbpaloqhHfaPTRMjCmSsJVZkr5w1vya36+tktfx5M/ndIctj6YkwnLzwypox
6eLufv3t/ubhIVmuVuvH5eP39eqi59evUClk/ZSsnOOcks42TMk6lScStebKilKz8sw4KBSNzD2l
Vris62NlR5CwvvE01DZlMnweiQuDc/rqe9xgxG2l4TzDHg4f4WoKMWg04TF9kzYVvOa3DYuaGnpV
soTbE46gw57NTzwd89T178RSnwmPqciEF5PFG4gG1bec3gUOxRjMyQLIu7B1TE+Ta8/LWc+p9WUc
YiDnuTnhtpwdjp7l0FfOAFW8GsJp9ObP+tZ/ysuiHsEzY30Ysg+Qo/fK5OC32MIGf4RWO2SX4l1E
kFlZFezHbA5kQXmgra10BinChtdMG3LrfUmLOO5XYqRszGyKvaAdxUJKlvtSlKVWsmnE5XFntK72
GgxEsCXP6yTeCxe7ysSEkh306hVCt1cSOYytjI+7SibTyKHI+Hf+D1BLAwQUAAAACACrSXdRLBwC
BJEAAAD2AAAAOAAAAHBhcGVybWlsbF9rdWJlcm5ldGVzX2pvYl9wcm9ncmVzcy0wLjEuZGlzdC1p
bmZvL01FVEFEQVRBXY+xDgIhEER7voIfWKOWdCYWJioajVrv6Xqi7IELFP6913iYK+dNXjKzpYw3
zAhnkuRCZ/R8MlMWmYyOGEnYeQ+v0pB0lCnBMzQQJbRCKalBmvbSsTCjfIw+2bXdXaxaBSaI2FJF
i5IfQcYZiNH5SjfuSl360/Ye8z0IV3Kgd3H9Bli6lI2uA8fNcEKpn6vUF1BLAwQUAAAACACrSXdR
+ow4flwAAABcAAAANQAAAHBhcGVybWlsbF9rdWJlcm5ldGVzX2pvYl9wcm9ncmVzcy0wLjEuZGlz
dC1pbmZvL1dIRUVMC89ITc3RDUstKs7Mz7NSMNQz4HJPzUstSizJL7JSSErJLC6JLwepUdAw0DM2
0TPS5ArKzy/R9SzWDSgtSs3JTLJSKCkqTeUKSUy3UiioNNbNy89L1U3Mq+TiAgBQSwMEFAAAAAgA
q0l3UTu6zoNDAAAAbAAAAEAAAABwYXBlcm1pbGxfa3ViZXJuZXRlc19qb2JfcHJvZ3Jlc3MtMC4x
LmRpc3QtaW5mby9lbnRyeV9wb2ludHMudHh0iy5ILEgtys3MydFLzUvPzEuN5couTUotykstSS2O
z8pPii8oyk8vSi0uVrBVgKuNx6HGyhsu7pWfFAAVdQUbzMUFAFBLAwQUAAAACACrSXdR4BgiriQA
AAAiAAAAPQAAAHBhcGVybWlsbF9rdWJlcm5ldGVzX2pvYl9wcm9ncmVzcy0wLjEuZGlzdC1pbmZv
L3RvcF9sZXZlbC50eHQrSCxILcrNzMmJzy5NSi3KSy1JLY7Pyk+KLyjKTy9KLS7mAgBQSwMEFAAA
AAgAq0l3Ua50RMBCAQAAZwIAADYAAABwYXBlcm1pbGxfa3ViZXJuZXRlc19qb2JfcHJvZ3Jlc3Mt
MC4xLmRpc3QtaW5mby9SRUNPUkSVzUtygjAAANB9zwKKfGXRBUL8VNSqqNBNBiFgEEJMAoKn78qZ
7jq8CzwaU8QqXJbw3lwRI0ggDov6Cimrc4Y4H0OICRYQjmgv8VusGuandQSa7QVq4fhxATGg1qFD
pDtb/VPtki+0wzw00ufpxBJpopvGB/1vkZXRZJRiLmRMsnq8AYHjOYHzDhl63DXvAdr1sa1mD/Pc
arMVan4A3G7ml5dxt5N09u3IyiyXVN0c/F2WAPjvLNfJhlupHJa23Ll2WLcnxr3lIhTyPFDAxIoe
ta16IGsjyVYHX4gI1kNaYyL4SHTi3fphQjK/cUy98kWy0td9dI5z1Q732H0tNflYACMmLsumujRR
poNfUVNYohaVf9MY1FTJV7ddt26bxV0EWW8eouqix94mxntNYddn4W8rrTlJmj74PAB3d/Ak6eMX
UEsBAhQDFAAAAAgAgUl3UfBoHqBQAgAAuQUAAC0AAAAAAAAAAAAAALSBAAAAAHBhcGVybWlsbF9r
dWJlcm5ldGVzX2pvYl9wcm9ncmVzcy9fX2luaXRfXy5weVBLAQIUAxQAAAAIAKtJd1EsHAIEkQAA
APYAAAA4AAAAAAAAAAAAAAC0gZsCAABwYXBlcm1pbGxfa3ViZXJuZXRlc19qb2JfcHJvZ3Jlc3Mt
MC4xLmRpc3QtaW5mby9NRVRBREFUQVBLAQIUAxQAAAAIAKtJd1H6jDh+XAAAAFwAAAA1AAAAAAAA
AAAAAAC0gYIDAABwYXBlcm1pbGxfa3ViZXJuZXRlc19qb2JfcHJvZ3Jlc3MtMC4xLmRpc3QtaW5m
by9XSEVFTFBLAQIUAxQAAAAIAKtJd1E7us6DQwAAAGwAAABAAAAAAAAAAAAAAAC0gTEEAABwYXBl
cm1pbGxfa3ViZXJuZXRlc19qb2JfcHJvZ3Jlc3MtMC4xLmRpc3QtaW5mby9lbnRyeV9wb2ludHMu
dHh0UEsBAhQDFAAAAAgAq0l3UeAYIq4kAAAAIgAAAD0AAAAAAAAAAAAAALSB0gQAAHBhcGVybWls
bF9rdWJlcm5ldGVzX2pvYl9wcm9ncmVzcy0wLjEuZGlzdC1pbmZvL3RvcF9sZXZlbC50eHRQSwEC
FAMUAAAACACrSXdRrnREwEIBAABnAgAANgAAAAAAAAAAAAAAtAFRBQAAcGFwZXJtaWxsX2t1YmVy
bmV0ZXNfam9iX3Byb2dyZXNzLTAuMS5kaXN0LWluZm8vUkVDT1JEUEsFBgAAAAAGAAYAYQIAAOcG
AAAAAA=="""

install_papermill_kubernetes_job_progress = (
    f'echo "{papermill_kubernetes_job_progress_wheel_base64}" | '
    f" base64 -d > /tmp/{papermill_kubernetes_job_progress_wheel_name} && "
    f"python3 -m pip install /tmp/{papermill_kubernetes_job_progress_wheel_name} "
)
