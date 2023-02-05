from prefect.deployments import Deployment
from etl_web_to_gcs import etl_parent_flow

basic_deployment = Deployment.build_from_flow(
    flow=etl_parent_flow, name="homework-flow"
)

if __name__ == "__main__":
    basic_deployment.apply()
