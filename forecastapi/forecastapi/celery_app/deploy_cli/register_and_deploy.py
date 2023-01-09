import click
from mlflow.tracking import MlflowClient
import mlflow


@click.command()
@click.option(
    "--run-id",
    "-r",
    default=None,
    type=str,
    help=" If specified, get the run with the specified UUID and register "
         "the model under that run."
)
@click.option(
    "--run-name",
    "-rn",
    default=None,
    type=str,
    help="Name of run. Used only when run_id is unspecified"
)
@click.option(
    "--stage",
    "-s",
    default='Staging',
    type=str,
    help="New desired stage for this model version. Default=Staging"
)
@click.option(
    "--artifact-path",
    "-a",
    default='model',
    type=str,
    help="Run-relative artifact path. Default='pipeline'"
)
@click.option(
    "--archive-existing-versions",
    "-aev",
    default=True,
    type=bool,
    help="If this flag is set to ``True``, all existing model versions in the "
         "stage will be automatically moved to the 'archived'. stage. Only "
         "valid when ``stage`` is ``'staging'`` or ``'production'`` "
         "otherwise an error will be raised."
)
@click.option(
    "--model-name",
    "-mn",
    default=None,
    type=str,
    help="Name of the registered model under which to create a new model "
         "version. If a registered model with the given name does not exist, "
         "it will be created automatically. If not specified, the run name "
         "will be used."
)
@click.option(
    "--image-name",
    "-in",
    type=str,
    default=None,
    help="Name to use for built image. If not specified, the run name will be "
         "used."
)
def register_and_deploy(
        run_id,
        run_name,
        stage,
        artifact_path,
        archive_existing_versions,
        model_name
):
    if run_id is None:
        run_id = _search_run_id(run_name)
    if run_name is None:
        run_name = _search_run_name(run_id)

    # Register model.
    model_uri = _make_run_model_uri(run_id, artifact_path)
    if model_name is None:
        model_name = run_name
    model_version = mlflow.register_model(model_uri, model_name)

    # Transition to stage.
    client = MlflowClient()
    client.transition_model_version_stage(
        name=model_name,
        version=model_version.version,
        stage=stage,
        archive_existing_versions=archive_existing_versions
    )
    model_uri = _make_register_model_uri(name, stage)
    mlflow.models.build_docker(model_uri, image_name)


def _make_run_model_uri(run_id, artifact_path):
    return f"runs:/{run_id}/{artifact_path}"


def _make_register_model_uri(name, stage):
    return f"models:/{name}/{stage}"


def _search_run_id(run_name):
    filter_string = f'tags.mlflow.runName = "{run_name}"'
    return mlflow.search_runs(filter_string=filter_string).iloc[0].run_id


if __name__ == '__main__':
    register_and_deploy()
