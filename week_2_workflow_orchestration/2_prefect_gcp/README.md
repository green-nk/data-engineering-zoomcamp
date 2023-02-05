# Prefect on GCP

## Commands
Basic commands to build a workflow with Prefect.


### Introduction to Prefect ?
[Prefect](https://docs.prefect.io/) is an open-source workflow orchestration tool. There are several concepts that help us to run better workflows such as [Prefect Flows](https://docs.prefect.io/concepts/flows/), [Prefect Tasks](https://docs.prefect.io/concepts/tasks/), [Prefect Blocks](https://docs.prefect.io/concepts/blocks/) and [Prefect Deployment](https://docs.prefect.io/concepts/deployments/). See [concepts](https://docs.prefect.io/concepts/overview/) for details.


### How to run prefect ?
Just wrap your python code with `flows` and `tasks` to get started.
```bash
python ingest.py
```
Note that `GCS Block` and `GCP Credential Block` are conveniet way to set up connections between different parties.


### How to run a UI interface locally ?
```bash
prefect orion start
```
Note that there is a [(free) cloud version](https://app.prefect.cloud) if you don't want to run it on local.
