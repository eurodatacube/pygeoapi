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
from pathlib import PurePath, Path
import os
import re
import scrapbook
import scrapbook.scraps
from typing import Dict, Iterable, Optional, List, Tuple, Any
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

# this just needs to be any unique id
JOB_RUNNER_GROUP_ID = 20200


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

        # TODO: config file parsing (typed-json-dataclass?)
        self.default_image: str = processor_def["default_image"]
        self.image_pull_secret: str = processor_def["image_pull_secret"]
        self.s3: Optional[Dict[str, str]] = processor_def.get("s3")
        self.home_volume_claim_name: str = processor_def["home_volume_claim_name"]
        self.extra_pvcs: List = processor_def["extra_pvcs"]
        self.jupyer_base_url: str = processor_def["jupyter_base_url"]
        self.output_directory: Path = Path(processor_def["output_directory"])

    def create_job_pod_spec(
        self,
        data: Dict,
        job_name: str,
    ) -> KubernetesProcessor.JobPodSpec:
        LOGGER.debug("Starting job with data %s", data)
        # TODO: add more formal data parsing (typed-json-dataclass?)
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

        output_notebook = setup_output_notebook(
            output_directory=self.output_directory,
            output_notebook_filename=PurePath(
                data.get("output_filename", default_output_path(notebook_path))
            ).name,
        )

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

        # NOTE: this link currently doesn't work (even those created in
        #   the ui with "create sharable link" don't)
        #   there is a recently closed issue about it:
        # https://github.com/jupyterlab/jupyterlab/issues/8359
        #   it doesn't say when it was fixed exactly. there's a possibly
        #   related fix from last year:
        # https://github.com/jupyterlab/jupyterlab/pull/6773
        result_link = (
            f"{self.jupyer_base_url}/hub/user-redirect/lab/tree/"
            + urllib.parse.quote(str(output_notebook.relative_to(CONTAINER_HOME)))
        )

        # save parameters but make sure the string is not too long
        extra_annotations = {
            "parameters": b64decode(parameters).decode()[:8000],
            "result-link": result_link,
            "result-notebook": str(output_notebook),
        }

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
                security_context=k8s_client.V1PodSecurityContext(
                    supplemental_groups=[JOB_RUNNER_GROUP_ID]
                ),
                **extra_podspec,
            ),
            extra_annotations=extra_annotations,
        )

    def __repr__(self):
        return "<PapermillNotebookKubernetesProcessor> {}".format(self.name)


def notebook_job_output(result: Dict) -> Tuple[Any, Optional[str]]:

    # NOTE: this assumes that we have user home under the same path as jupyter
    scraps = scrapbook.read_notebook(result["result-notebook"]).scraps

    LOGGER.debug("Retrieved scraps from notebook: %s", scraps)

    if not scraps:
        return ({"result-link": result["result-link"]}, None)
    elif len(scraps) == 1:
        # if there's only one item, return it right away with correct content type.
        # this way, you can show e.g. an image in the browser.
        # otherwise, we just return the scrap structure
        return serialize_single_scrap(next(iter(scraps.values())))
    else:
        return scraps, None


def serialize_single_scrap(scrap: scrapbook.scraps.Scrap) -> Tuple[Any, Optional[str]]:

    text_mime = "text/plain"

    if scrap.display:
        # we're only interested in display_data
        # https://ipython.org/ipython-doc/dev/notebook/nbformat.html#display-data

        if scrap.display["output_type"] == "display_data":
            # data contains representations with different mime types as keys. we
            # want to prefer non-text
            mime_type = next(
                (f for f in scrap.display["data"] if f != text_mime),
                text_mime,
            )
            item = scrap.display["data"][mime_type]
            encoded_output = item if mime_type == text_mime else b64decode(item)
            return (encoded_output, mime_type)
        else:
            return scrap.display, None
    else:
        return scrap.data, None


def default_output_path(notebook_path: str) -> str:
    filename_without_postfix = re.sub(".ipynb$", "", notebook_path)
    now_formatted = datetime.now().strftime("%Y%m%d-%H%M%S-%f")
    return filename_without_postfix + f"_result_{now_formatted}.ipynb"


def setup_output_notebook(
    output_directory: Path,
    output_notebook_filename: str,
) -> Path:
    output_notebook = output_directory / output_notebook_filename

    # create output directory owned by root (readable, but not changeable by user)
    output_directory.mkdir(exist_ok=True, parents=True)

    # create file owned by root but group is job runner group.
    # this way we can execute the job with the jovyan user with the additional group,
    # which the actual user (as in human) in jupyterlab does not have.
    output_notebook.touch(exist_ok=False)
    # TODO: reasonable error when output notebook already exists
    os.chown(output_notebook, uid=0, gid=JOB_RUNNER_GROUP_ID)
    os.chmod(output_notebook, mode=0o664)
    return output_notebook


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
papermill_kubernetes_job_progress_wheel_base64 = """UEsDBBQAAAAIAPpjd1E8FHoBdwIAACYGAAAtAAAAcGFwZXJtaWxsX2t1YmVybmV0ZXNfam9iX3By
b2dyZXNzL19faW5pdF9fLnB5fVRNb9swDL37VxDpxc5Su7kNAQYsHYphHZYUa7HLMBiyzDhaZMmQ
5LTA0P8+yom/krQ6WSQfP56fKMpKGwfaBsHG6BJ2dYZGoUML4uDiUqBywCzsPtr0cJsB12ojis7a
3A4ZKlahKYWUMapCqD7R6vZLA75rzEEQcMmshe9dxXudPRhdGLT2EBOOIdEiADqfG1yJbqvzxpDj
BvAFee0wLZliBeap0g4zrXchl3YGKvOOGUyZKeg6ne6e/RclbBL4048RS83yVCgua+vQHK1h1IVm
zPFtup/DpwEl8a23/povK0GhXexfnaWKlUix2hIje2G0+j25X9+mq+WPu8mfLrI6jp4yRc0zJ7Q6
AT38XH/9eff4mC5Xq/XT8unbejXA+yq2YtyX4rUx1FPa2YYtaSOKlKOUNFlZSWKeEAeG4pG5h3iG
Kz8fMTsKCf2N1OBtQuX4MiIXBue86nvYYIQ1no20JYi6DXsQfIB5BAlIVOFxCJU1c9holOSqYxiE
BQZ7JmuEDN0zooIbYCqH+c3NCDMoaXSt8vCkk6lHnDTbqiNuCOt/QZ6SFsJR6FAgszNPhzx3/Tuz
+DOhN8Fy5thk8UZEE9Xry74bOCRgIMoFWGfC1hGdN9ee14uec+vrOMWAzkuipF9xUYk9yqCrjQJb
0x4Ko/jNzfDWUqDN5PV+4Q0dFH0FBTonVAFui23Y4PlJsUNyiUZnueZ1SX7MZ2A1CAd2q2uZk/Bg
QzutTbl1rrKLJOn3byx0QmibOGZ3NmGcE93XrKqk4M2PuD4uqNbVXoMBCbqiZzFJ9swkplaJRU4O
e1LFotkLjpSGNO6SbpJJFBtkOe2O/1BLAwQUAAAACAATZHdRLBwCBJEAAAD2AAAAOAAAAHBhcGVy
bWlsbF9rdWJlcm5ldGVzX2pvYl9wcm9ncmVzcy0wLjEuZGlzdC1pbmZvL01FVEFEQVRBXY+xDgIh
EER7voIfWKOWdCYWJioajVrv6Xqi7IELFP6913iYK+dNXjKzpYw3zAhnkuRCZ/R8MlMWmYyOGEnY
eQ+v0pB0lCnBMzQQJbRCKalBmvbSsTCjfIw+2bXdXaxaBSaI2FJFi5IfQcYZiNH5SjfuSl360/Ye
8z0IV3Kgd3H9Bli6lI2uA8fNcEKpn6vUF1BLAwQUAAAACAATZHdR+ow4flwAAABcAAAANQAAAHBh
cGVybWlsbF9rdWJlcm5ldGVzX2pvYl9wcm9ncmVzcy0wLjEuZGlzdC1pbmZvL1dIRUVMC89ITc3R
DUstKs7Mz7NSMNQz4HJPzUstSizJL7JSSErJLC6JLwepUdAw0DM20TPS5ArKzy/R9SzWDSgtSs3J
TLJSKCkqTeUKSUy3UiioNNbNy89L1U3Mq+TiAgBQSwMEFAAAAAgAE2R3UTu6zoNDAAAAbAAAAEAA
AABwYXBlcm1pbGxfa3ViZXJuZXRlc19qb2JfcHJvZ3Jlc3MtMC4xLmRpc3QtaW5mby9lbnRyeV9w
b2ludHMudHh0iy5ILEgtys3MydFLzUvPzEuN5couTUotykstSS2Oz8pPii8oyk8vSi0uVrBVgKuN
x6HGyhsu7pWfFAAVdQUbzMUFAFBLAwQUAAAACAATZHdR4BgiriQAAAAiAAAAPQAAAHBhcGVybWls
bF9rdWJlcm5ldGVzX2pvYl9wcm9ncmVzcy0wLjEuZGlzdC1pbmZvL3RvcF9sZXZlbC50eHQrSCxI
LcrNzMmJzy5NSi3KSy1JLY7Pyk+KLyjKTy9KLS7mAgBQSwMEFAAAAAgAE2R3UaxOWShCAQAAZwIA
ADYAAABwYXBlcm1pbGxfa3ViZXJuZXRlc19qb2JfcHJvZ3Jlc3MtMC4xLmRpc3QtaW5mby9SRUNP
UkSV0UtygjAAANC9ZwGN/JRFFyjxU0ErokA3mSgBo0AgCVQ8fVfOdNfxBG/xalwTXtKiQPf2THhF
JBHoxs6o5iznRIgRQrSiEqFh3SviijXT+lhg3jQ++GSrWCDNCxwgUUhuHu36NZymQTxlEXCSCPNc
GZsTY1D/p6hgOB6mVEiVVhkb+TB0XCd0XiAnzV13G9htDl05a6xTp8/WpP2GaOsvoqd5ty/p7MtR
wSxXNMN624tWEHovLDcqX0xSNS5s9TG3Y9YduXBXy1iqixDA8SRpmK25MOsSxdbetkgleY9qRisp
hvIhX6wXX6rMax3LKD15WRubPjnhXLPjPZ0/V7p6uEETV3OeTQ1lDKZvu5LVqCAdKf6iGLIa5Ovr
7rHp2uVdhllvBUkZGdj1Md3rgJ9/bt621Nujor//GMD5LnAVZfALUEsBAhQDFAAAAAgA+mN3UTwU
egF3AgAAJgYAAC0AAAAAAAAAAAAAALSBAAAAAHBhcGVybWlsbF9rdWJlcm5ldGVzX2pvYl9wcm9n
cmVzcy9fX2luaXRfXy5weVBLAQIUAxQAAAAIABNkd1EsHAIEkQAAAPYAAAA4AAAAAAAAAAAAAAC0
gcICAABwYXBlcm1pbGxfa3ViZXJuZXRlc19qb2JfcHJvZ3Jlc3MtMC4xLmRpc3QtaW5mby9NRVRB
REFUQVBLAQIUAxQAAAAIABNkd1H6jDh+XAAAAFwAAAA1AAAAAAAAAAAAAAC0gakDAABwYXBlcm1p
bGxfa3ViZXJuZXRlc19qb2JfcHJvZ3Jlc3MtMC4xLmRpc3QtaW5mby9XSEVFTFBLAQIUAxQAAAAI
ABNkd1E7us6DQwAAAGwAAABAAAAAAAAAAAAAAAC0gVgEAABwYXBlcm1pbGxfa3ViZXJuZXRlc19q
b2JfcHJvZ3Jlc3MtMC4xLmRpc3QtaW5mby9lbnRyeV9wb2ludHMudHh0UEsBAhQDFAAAAAgAE2R3
UeAYIq4kAAAAIgAAAD0AAAAAAAAAAAAAALSB+QQAAHBhcGVybWlsbF9rdWJlcm5ldGVzX2pvYl9w
cm9ncmVzcy0wLjEuZGlzdC1pbmZvL3RvcF9sZXZlbC50eHRQSwECFAMUAAAACAATZHdRrE5ZKEIB
AABnAgAANgAAAAAAAAAAAAAAtAF4BQAAcGFwZXJtaWxsX2t1YmVybmV0ZXNfam9iX3Byb2dyZXNz
LTAuMS5kaXN0LWluZm8vUkVDT1JEUEsFBgAAAAAGAAYAYQIAAA4HAAAAAA=="""

install_papermill_kubernetes_job_progress = (
    f'echo "{papermill_kubernetes_job_progress_wheel_base64}" | '
    f" base64 -d > /tmp/{papermill_kubernetes_job_progress_wheel_name} && "
    f"python3 -m pip install /tmp/{papermill_kubernetes_job_progress_wheel_name} "
)
