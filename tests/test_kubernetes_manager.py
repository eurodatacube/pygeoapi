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

import pytest
from unittest import mock
from kubernetes import client as k8s_client

from pygeoapi.process.manager.kubernetes import (
    KubernetesManager,
)


@pytest.fixture()
def mock_k8s_base():
    with mock.patch("pygeoapi.process.manager.kubernetes.k8s_config"):
        with mock.patch("pygeoapi.process.manager.kubernetes.current_namespace"):
            yield


@pytest.fixture()
def mock_read_job():
    with mock.patch(
        "pygeoapi.process.manager.kubernetes.k8s_client.BatchV1Api.read_namespaced_job",
        return_value=k8s_client.V1Job(
            spec=k8s_client.V1JobSpec(
                template="",
                selector=k8s_client.V1LabelSelector(match_labels={}),
            ),
            metadata=k8s_client.V1ObjectMeta(
                annotations={"pygeoapi.io/result-notebook": "/a/b/a.ipynb"}
            ),
            status=k8s_client.V1JobStatus(),
        ),
    ):
        yield


@pytest.fixture()
def mock_list_pods():
    with mock.patch(
        "pygeoapi.process.manager.kubernetes.k8s_client.CoreV1Api.list_namespaced_pod",
        return_value=k8s_client.V1PodList(
            items=[
                k8s_client.V1Pod(
                    status=k8s_client.V1PodStatus(
                        container_statuses=[
                            k8s_client.V1ContainerStatus(
                                image="a",
                                image_id="b",
                                name="c",
                                ready=True,
                                restart_count=0,
                                state=k8s_client.V1ContainerState(
                                    terminated=k8s_client.V1ContainerStateTerminated(
                                        exit_code=0
                                    )
                                ),
                            )
                        ],
                    )
                )
            ]
        ),
    ):
        yield


@pytest.fixture()
def mock_delete_job():
    with mock.patch(
        "pygeoapi.process.manager.kubernetes.k8s_client.BatchV1Api.delete_namespaced_job",
    ) as m:
        yield m


@pytest.fixture()
def manager(mock_k8s_base) -> KubernetesManager:
    return KubernetesManager({"name": "kman"})


def test_deleting_job_deletes_in_k8s_and_on_nb_file_on_disc(
    manager: KubernetesManager,
    mock_read_job,
    mock_list_pods,
    mock_delete_job,
):
    with mock.patch("pygeoapi.process.manager.kubernetes.os.remove") as mock_os_remove:
        result = manager.delete_job(1, 2)

    assert result
    mock_delete_job.assert_called_once()
    mock_os_remove.assert_called_once()
