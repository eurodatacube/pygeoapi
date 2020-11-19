import os

from kubernetes import client as k8s_client, config as k8s_config
from papermill.engines import NBClientEngine


class KubernetesJobProgressEngine(NBClientEngine):
    @classmethod
    def execute_managed_notebook(cls, nb_man, *args, **kwargs):

        k8s_config.load_incluster_config()
        batch_v1 = k8s_client.BatchV1Api()

        job_name = os.environ["JOB_NAME"]
        namespace = current_namespace()

        orig_cell_complete = nb_man.cell_complete

        def patched_cell_complete(cell, cell_index, **kwargs):
            orig_cell_complete(cell, cell_index, **kwargs)

            progress = (cell_index + 1) / len(nb_man.nb.cells)

            batch_v1.patch_namespaced_job(
                job_name,
                namespace,
                {
                    "metadata": {
                        "annotations": {
                            # TODO: fix tag
                            "pygeoapi_progress": str(progress),
                        }
                    }
                },
            )

        nb_man.cell_complete = patched_cell_complete

        return super().execute_managed_notebook(nb_man, *args, **kwargs)


def current_namespace():
    # getting the current namespace like this is documented, so it should be fine:
    # https://kubernetes.io/docs/tasks/access-application-cluster/access-cluster/
    return open("/var/run/secrets/kubernetes.io/serviceaccount/namespace").read()
