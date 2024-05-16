#!/usr/bin/env -S python3 -u
import json
import sys
from os import environ
from pathlib import Path
from subprocess import run
from typing import NamedTuple


class Config(NamedTuple):
    pipeline_spec: Path
    dockerfile: Path
    docker_context: Path
    image_name: str


def main(config_file: Path):
    config = parse_config(config_file)
    sha = environ.get("GITHUB_SHA")
    tagged_image = f"{config.image_name}:{sha}"

    if not check_pipeline_exists(config.pipeline_spec):
        build_and_update(config, tagged_image)
        exit(0)

    diff = git_diff()
    print(f"> git diff: {diff}")
    if config.pipeline_spec.resolve() in diff:
        build_and_update(config, tagged_image)
        exit(0)

    build_context = list_docker_context(config.docker_context.resolve())
    print(f"> build context: {build_context}")
    for file in diff:
        if file in build_context:
            break
    else:
        print(f"No changes detected to the files within the docker build context: {config.docker_context}")
        print("Exiting without updating pipeline")
        exit(0)

    build_and_update(config, tagged_image)


def parse_config(config_file: Path) -> Config:
    parsed = json.loads(config_file.read_bytes())
    return Config(
        pipeline_spec=config_file.parent.joinpath(Path(parsed["pipeline_spec"])),
        dockerfile=config_file.parent.joinpath(Path(parsed["dockerfile"])),
        docker_context=config_file.parent.joinpath(Path(parsed["build_dir"])),
        image_name=parsed["image_name"],
    )

def check_pipeline_exists(pipeline_spec: Path) -> bool:
    from pachyderm_sdk import Client
    from pachyderm_sdk.api import pps, pfs

    parsed = json.loads(pipeline_spec.read_bytes())
    project = parsed["pipeline"].get("project")
    if project is not None:
        project = project.get("name", "default")
    name = parsed["pipeline"].get("name")
    pipeline = pps.Pipeline(name=name, project=pfs.Project(name=project))

    client = Client.from_pachd_address(environ.get("PACHYDERM_CLUSTER_URL"))
    return client.pps.pipeline_exists(pipeline)


def git_diff() -> list[Path]:
    process = run("git diff --name-only HEAD^..HEAD".split(' '), capture_output=True)
    return [Path(file).resolve() for file in process.stdout.decode().splitlines()]


def list_docker_context(build_dir: Path) -> list[Path]:
    command = f"rsync -avn {build_dir} /dev/shm"
    if (docker_ignore := build_dir / ".dockerignore").exists():
        command += f" --exclude-from {docker_ignore}"
    process = run(command.split(' '), capture_output=True)
    return [Path(file).resolve() for file in process.stdout.decode().splitlines() if file.startswith(build_dir.name)]


def build_image(image_name: str, dockerfile: Path, docker_context: Path) -> None:
    print(f" > building: {image_name}")
    run(f"docker build --tag {image_name} --file {dockerfile.resolve()} {docker_context}".split(), check=True)


def push_image(image_name: str) -> None:
    username = environ.get("DOCKERHUB_USERNAME")
    token = environ.get("DOCKERHUB_TOKEN")
    run(f"docker login --username {username} --password {token}".split(), check=True)
    print(f" > pushing: {image_name}")
    run(f"docker push {image_name}".split(), check=True)


def update_pipeline(pipeline_spec: Path, image_name: str) -> None:
    from pachyderm_sdk import Client
    from pachyderm_sdk.api import pps

    parsed = json.loads(pipeline_spec.read_bytes())
    parsed["transform"]["image"] = image_name

    client = Client.from_pachd_address(environ.get("PACHYDERM_CLUSTER_URL"))
    client.pps.create_pipeline_v2(
        create_pipeline_request_json=json.dumps(parsed),
        update=True,
    )


def build_and_update(config: Config, tagged_image: str) -> None:
    build_image(tagged_image, config.dockerfile, config.docker_context)
    push_image(tagged_image)
    update_pipeline(config.pipeline_spec, tagged_image)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("No argument provided. Expected config file path.")
        exit(1)
    _config_file = Path(sys.argv[1])
    if not _config_file.is_file():
        print(f"Config file does not exist: {_config_file}")
        exit(1)
    main(_config_file.resolve())
