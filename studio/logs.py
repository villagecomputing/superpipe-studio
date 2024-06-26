from studio.client import get_client
from studio.urls import logs_insert_url


def insert_log(
    inputs,
    steps,
    final_output_columns,
    accuracy,
    fingerprint,
    name,
    parameters
):
    data = {
        "inputs": inputs,
        "steps": steps,
        "final_output_columns": final_output_columns,
        "accuracy": accuracy,
        "fingerprint": fingerprint,
        "name": name,
        "parameters": parameters
    }
    client = get_client()
    response = client.post(logs_insert_url, json=data)
    return response


def run_pipeline_with_log(run, pipeline):
    def run_with_log(input):
        inputs_list = [{"name": k, "value": str(v)} for k, v in input.items()]
        # note: the run function may modify the input in-place
        output = run(input)
        if pipeline.evaluation_fn is not None:
            accuracy = output[f"__{pipeline.evaluation_fn.__name__}__"]
        else:
            accuracy = None
        insert_log(
            inputs=inputs_list,
            steps=get_steps(pipeline, output),
            final_output_columns=pipeline.output_fields,
            accuracy=accuracy,
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
    } for field in step.output_fields()]
