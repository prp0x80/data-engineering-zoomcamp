from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import CronSchedule
from week2_homework1_solution.etl_web_to_gcs import etl_parent_flow

cron_deployment = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name="homework-cron-flow",
    schedule=(CronSchedule(cron="0 5 1 * *", timezone="UTC")),
)

if __name__ == "__main__":
    cron_deployment.apply()
