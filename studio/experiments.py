import pandas as pd
from studio.client import get_client
from studio.urls import experiment_insert_url, experiment_create_url


def create_experiment(
        dataset_id: str,
        name: str,
        parameters: dict,
        group_id: str,
        description: str = None):
    data = {
        "datasetId": dataset_id,
        "name": name,
        "parameters": parameters,
        "groupId": group_id,
        # "description": description TODO: handle sending up Nones
    }
    client = get_client()
    response = client.post(experiment_create_url, json=data)
    if response.status_code != 200:
        raise Exception(
            f"Creating experiment failed with response {response.status_code}: {response.json()}")
    experiment_id = response.json().get('id')
    return experiment_id


def experiment_insert(
    id,
    dataset_row_fingerprint,
    steps,
    final_output_columns,
    accuracy,
):
    data = {
        "dataset_row_fingerprint": dataset_row_fingerprint,
        "steps": steps,
        "final_output_columns": final_output_columns,
        "accuracy": accuracy,
    }
    client = get_client()
    response = client.post(experiment_insert_url(id), json=data)
    if response.status_code != 200:
        raise Exception(
            f"Inserting experiment rows failed with response {response.status_code}: {response.json()}")
    return response.json()


def run_pipeline_with_experiment(experiment_id, run, pipeline):
    def run_with_log(row: pd.Series):
        # note: the run function may modify the input in-place
        output = run(row)
        experiment_insert(
            id=experiment_id,
            dataset_row_fingerprint=row.name,
            steps=get_steps(pipeline, output),
            final_output_columns=pipeline.output_fields,
            accuracy=0)  # TODO
        return output

    return run_with_log


def get_steps(pipeline, output):
    steps = []
    for step in pipeline.steps:
        steps.append({
            "name": step.name,
            "metadata": get_metadata_for_step(step, output),
            "outputs": get_outputs_for_step(step, output),
        })
    return steps


def get_metadata_for_step(step, output):
    return output[f"__{step.name}__"]


def get_outputs_for_step(step, output):
    return [{
        "name": field,
        "value": str(output[field])
    } for field in step.output_fields()]
