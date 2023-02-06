from prefect.deployments import Deployment
from prefect.filesystems import GitHub 
from flow import main


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
