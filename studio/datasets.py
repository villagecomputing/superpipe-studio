import json
import pandas as pd
from studio.urls import dataset_fetch_url, dataset_create_url, dataset_add_url
from studio.client import get_client
from studio.util import hash16
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
    if response.status_code != 200:
        raise Exception(
            f"Creating dataset failed with response {response.status_code}: {response.json()}")
    dataset_id = response.json().get('id')
    upload_url = dataset_add_url(dataset_id)
    dataset_rows = data.apply(lambda x: {
        "fingerprint": x.name,
        **json.loads(x.astype(str).to_json())
    }, axis=1).to_list()
    _json = {"datasetRows": dataset_rows}
    response = client.post(upload_url, json=_json)
    if response.status_code != 200:
        raise Exception(
            f"Adding data to dataset failed with response {response.status_code}: {response.json()}")
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
            self._create_fingerprints(data)
            self.id = create(data, name, ground_truths)
            self.data = data
        elif id is not None:
            self.data = download(id)
            self.id = id
        else:
            raise ValueError("Either data or id should be provided")

    def _create_fingerprints(self, df: pd.DataFrame):
        df.index = df.apply(lambda x: hash16(x.astype(str).to_json()), axis=1)

    def add_data(self, data: pd.DataFrame):
        self._create_fingerprints(data)
        if set(self.data.columns) != set(data.columns):
            raise ValueError(
                "Columns of the new data do not match the columns of the dataset")
        add_data(self.id, data)
        self.data = pd.concat([self.data, data])
