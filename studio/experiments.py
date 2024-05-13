from studio.client import get_client
from studio.urls import experiment_insert_url, experiment_create_url


def create_experiment(
        dataset_id: str,
        name: str,
        parameters: dict,
        description: str = None):
    data = {
        "dataset_id": dataset_id,
        "name": name,
        "parameters": parameters,
        "description": description
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
    def run_with_log(input):
        # note: the run function may modify the input in-place
        output = run(input)
        experiment_insert(
            id=experiment_id,
            index=0,  # TODO
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
