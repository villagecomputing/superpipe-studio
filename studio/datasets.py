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


def create(data: pd.DataFrame, name: str):
    response = client.post(dataset_create_url, json={
        "datasetName": name,
        "columns": data.columns.tolist(),
        "groundTruths": []
    })
    dataset_id = response.json().get('id')
    upload_url = dataset_add_url(dataset_id)
    response = client.post(upload_url, json=data.to_dict(orient='records'))
    return dataset_id


def add_data(id, data: Union[pd.DataFrame, List[dict]]):
    upload_url = dataset_add_url(id)
    response = client.post(upload_url, json=data.to_dict(orient='records'))
    return response


class Dataset:
    def __init__(self,
                 data: pd.DataFrame = None,
                 id: str = None):
        if data:
            self.id = create(data)
            self.data = data
        elif id:
            self.data = download(id)
            self.id = id
        else:
            raise ValueError("Either data or id should be provided")

    def add_data(self, data: Union[pd.DataFrame, List[dict]]):
        return add_data(self.id, data)
