# Prefect Deployment

## Commands
Basic commands to build a workflow deployment with Prefect.


### How to run a Prefect flow manually ?
Pass arguments at run-time.
```bash
python flow.py \
    --taxi-color yellow \
    --years 2021 \
    --months 1 2 3 \
    --data-lake gcs-data-lake \
    --creds gcs-data-lake-creds \
    --project-id "dtc-de-375601" \
    --dataset trips_data_all \
    --table rides
```


### How to build & apply a deployment ?
There are several benefits to run a `deployment` instead of a `flow` such as scheduling and notification.

To review `deployment` concepts, there are two steps that we need to do to deploy our workflows. First, it is to `build` a deployment
```bash
prefect deployment build ./flow.py:main -n "Yellow NY Taxi Data Ingestion"
```

This step will create a `deployment.yaml` file. This is an instruction for Prefect to be used when running the flow. You can config some parameters in the file for example putting necessary inputs required by the script.
```yaml
...
parameters:
  config:
    taxi-color: yellow
    years:
    - 2019
    months:
    - 1
    - 2
    - 3
    data-lake: gcs-data-lake
    credentials: gcs-data-lake-creds
    project-id: dtc-de-375601
    dataset: trips_data_all
    table: rides
...
```

Note that you can determine `--cron` or `--interval` to schedule a `deployment` or `apply` once you already build a `deployment` using `-a` option.
```bash
prefect deployment build ./flow.py:main -n "Yellow NY Taxi Data Ingestion" --cron "0 0 * * *" -a
```
[crontab guru](https://crontab.guru/) is a guide to help build cron job.

You can add custom parameters via UI.
```json
{
  "taxi-color": "yellow", 
  "years": [2020], 
  "months": [1], 
  "data-lake": "gcs-data-lake", 
  "credentials": "gcs-data-lake-creds", 
  "project-id": "dtc-de-375601", 
  "dataset": "trips_data_all", 
  "table": "rides"
}
```

Now this workflow is on a staging area. The next step is to `apply` this flow to be ready to run.
```bash
prefect deployment apply main-deployment.yaml
```

You can do both steps via CLI or a python script. There is more flexiblity to do it on script such as
using `Github block` to store flow code.
```python
...
storage = GitHub.load("github-ny-taxi")

deployment = Deployment.build_from_flow(
    flow=main, 
    name="Green NY Taxi Data Dumping via GitHub", 
    parameters={"config": {"taxi-color": "green", "years": [2020], "months": [11], "data-lake": "gcs-data-lake", "credentials": "gcs-data-lake-creds", "project-id": "dtc-de-375601", "dataset": "trips_data_all", "table": "rides"}}, 
    storage=storage, 
    entrypoint="week_2_workflow_orchestration/3_deployment/flows/flow.py:main"
)

if __name__ == "__main__":
    deployment.apply()
```

And then run both to build and apply in the single reproducible script.
```bash
python deploy.py
```

For more details on command options
```bash
prefect deployment --help
```


### How to run a deployment ?
In order to you have to start an `agent` to pick up `a deployment` that in `work queues`.
```bash
prefect agent start --work-queue "default"
```
Note that you can add a `schedule` to specify how the `deployment` will run via UI.

Note that you can set up `notifications` via UI in case of any state needed to monitor. There are several options to choose from such as MS Team and Slacks.


### How to build & publish Docker image ?
Build an image with `name:tag`.
```bash
docker docker build -t idealenigma/prefect:prefect-de-zoomcamp .
```

Push to Docker hub
```bash
docker push idealenigma/prefect:prefect-de-zoomcamp
```

Note that in `Dockerfile`, there is the specific location for Prefect to retrive `flows`.
```Dockerfile
...
COPY flows /opt/prefect/flows
...
```

### How to run Docker containter with Prefect ?
First, you should create a `Docker Container Block` in order to keep configuration setup when spinning up a container.
```python
from prefect.infrastructure.docker import DockerContainer

container = DockerContainer(image="idealenigma/prefect:prefect-de-zoomcamp", image_pull_policy="ALWAYS", auto_remove=True)

container.save("docker-ny-taxi", overwrite=True)
```
Note that you can do this via UI too. This step is like preparing to run a containter with `docker run` command.

Next step, you should pack our deployment code into a single script and run to build & apply the `deployment`.
```bash
python flows/deploy.py
```


### Utility commands
To check Prefect `profile`
```bash
prefect profile ls
```

To set a local Orion API server explicitly
```bash
prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"
```

To refresh cache if the previous flow was failed
```bash
prefect config set PREFECT_TASKS_REFRESH_CACHE=True
```