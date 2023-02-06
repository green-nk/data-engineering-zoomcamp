from prefect.deployments import Deployment
from prefect.infrastructure.docker import DockerContainer
from flow import main


docker_block = DockerContainer.load("docker-ny-taxi")

deployment = Deployment.build_from_flow(
    flow=main, 
    name="Yellow NY Taxi Data Ingestion on Docker", 
    infrastructure=docker_block
)

if __name__ == "__main__":
    deployment.apply()
