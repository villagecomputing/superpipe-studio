from studio.client import get_client
from studio.urls import logs_insert_url


def insert_log(
    inputs,
    steps,
    accuracy,
    fingerprint,
    name,
    parameters
):
    data = {
        "inputs": inputs,
        "steps": steps,
        "accuracy": accuracy,
        "fingerprint": fingerprint,
        "name": name,
        "parameters": parameters
    }
    client = get_client()
    print("insert_log", data)
    response = client.post(logs_insert_url, json=data)
    return response


def run_pipeline_with_log(run, pipeline):
    def run_with_log(input):
        inputs_list = [{"name": k, "value": str(v)} for k, v in input.items()]
        # note: the run function may modify the input in-place
        output = run(input)
        insert_log(
            inputs=inputs_list,
            steps=get_steps(pipeline, output),
            accuracy=0,
            fingerprint=pipeline.fingerprint(deep=True),
            name=pipeline.name,
            parameters=pipeline.get_params())
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
    } for field in step.outputs()]
