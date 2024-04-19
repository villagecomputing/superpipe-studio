import os
from urllib.parse import urljoin

public_superpipe_url = "https://studio.superpipe.ai"
superpipe_studio_url = os.getenv("SUPERPIPE_STUDIO_URL", public_superpipe_url)
logs_insert_url = urljoin(superpipe_studio_url, "api/logs/insert")
