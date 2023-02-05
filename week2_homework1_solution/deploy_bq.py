from prefect.deployments import Deployment
from etl_gcs_to_bq import etl_gcs_to_bq_flow

bq_deployment = Deployment.build_from_flow(
    flow=etl_gcs_to_bq_flow, name="homework-bq-flow"
)

if __name__ == "__main__":
    bq_deployment.apply()
