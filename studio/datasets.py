import json
import pandas as pd
from studio.urls import dataset_fetch_url, dataset_create_url, dataset_add_url
from studio.client import get_client
from typing import Union, List

client = get_client()


def download(id):
    url = dataset_fetch_url(id)
    response = client.get(url)
    data = response.json()
    return pd.DataFrame(data)


def create(data: pd.DataFrame, name: str, ground_truths: List[str] = []):
    _json = {
        "datasetName": name,
        "columns": data.columns.tolist(),
        "groundTruths": ground_truths
    }
    response = client.post(dataset_create_url, json=_json)
    dataset_id = response.json().get('id')
    upload_url = dataset_add_url(dataset_id)
    _json = {
        "datasetRows": json.loads(data.astype(str).to_json(orient='records'))
    }
    response = client.post(upload_url, json=_json)
    return dataset_id


def add_data(id, data: Union[pd.DataFrame, List[dict]]):
    upload_url = dataset_add_url(id)
    _json = {
        "datasetRows": json.loads(data.astype(str).to_json(orient='records'))
    }
    response = client.post(upload_url, json=_json)
    return response


class Dataset:
    def __init__(self,
                 data: pd.DataFrame = None,
                 id: str = None,
                 name: str = None,
                 ground_truths: List[str] = []):
        if data is not None:
            if name is None:
                raise ValueError("Name must be provided")
            self.id = create(data, name, ground_truths)
            self.data = data
        elif id is not None:
            self.data = download(id)
            self.id = id
        else:
            raise ValueError("Either data or id should be provided")

    def add_data(self, data: Union[pd.DataFrame, List[dict]]):
        return add_data(self.id, data)
