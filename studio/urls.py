import os
from urllib.parse import urljoin

public_superpipe_url = "https://studio.superpipe.ai"
superpipe_studio_url = os.getenv("SUPERPIPE_STUDIO_URL", public_superpipe_url)
logs_insert_url = urljoin(superpipe_studio_url, "api/logs/insert")
dataset_create_url = urljoin(superpipe_studio_url, "api/dataset/new")
experiment_create_url = urljoin(superpipe_studio_url, "api/experiment/new")


def dataset_fetch_url(id): return urljoin(
    superpipe_studio_url, f"api/dataset/{id}")


def dataset_add_url(id): return urljoin(
    superpipe_studio_url, f"api/dataset/{id}/addData")


def experiment_insert_url(id): return urljoin(
    superpipe_studio_url, f"api/experiment/{id}/insert")
