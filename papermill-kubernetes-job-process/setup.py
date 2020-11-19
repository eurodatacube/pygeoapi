from setuptools import setup, find_packages

setup(
    name="papermill_kubernetes_job_progress",
    version="0.1",
    url="",
    packages=find_packages(),
    install_requires=["papermill", "kubernetes"],
    entry_points={
        "papermill.engine": [
            "kubernetes_job_progress=papermill_kubernetes_job_progress:KubernetesJobProgressEngine"
        ]
    },
)
