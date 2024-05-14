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
    experiment_id = response.json().get('id')
    return experiment_id


def experiment_insert(
    id,
    index,
    steps,
    final_output_columns,
    accuracy,
):
    data = {
        "index": index,
        "steps": steps,
        "final_output_columns": final_output_columns,
        "accuracy": accuracy,
    }
    client = get_client()
    response = client.post(experiment_insert_url(id), json=data)
    return response


def run_pipeline_with_experiment(experiment_id, run, pipeline):
    def run_with_log(row: pd.Series):
        # note: the run function may modify the input in-place
        output = run(row)
        experiment_insert(
            id=experiment_id,
            # TODO: this is a temporary hack, need to make sure the index lines up with the dataset index
            index=row.name + 1,
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
