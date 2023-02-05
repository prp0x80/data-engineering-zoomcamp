## Question 1

* Updated `flows/02_gcp/etl_web_to_gcs.py`
* Created new deployment using`python week2_homework1_solution/deploy.py`
* Trigerred the newly created deployment using Prefect UI with parameters - `month=1`, `year=2020`, and `color="green"`

> Answer: 447,770

## Question 2

* Created new deployment using `python week2_homework1_solution/deploy_cron.py`
* Triggered the newly created deployment manually using Prefect UI

> Answer: 0 5 1 * *

## Question 3

* Updated parameters to load February and March 2019 Yellow Taxi to GCS, and ran `python flows/02_gcp/etl_web_to_gcs.py`
* Created deployment using `python week2_homework1_solution/deploy_bq.py`
* Triggered the flow manually using the parameters - `months=[2, 3]`,`year=2019`, and `color="yellow"`

> Answer: 14,851,920

## Question 4

* Created a github block named `zoom-gh` using Prefect UI
* Created a new deployment using the following command (local infrastructure) -

```
prefect deployment build flows/02_gcp/etl_web_to_gcs.py:etl_web_to_gcs  
                         -n homework-gh-local  
                         -q default  
                         -sb github/zoom-gh  
                         -o local-gh-deployment  
                         --apply  
```

* Refer to `local-gh-deployment.yaml`
* Triggered a flow using newly created deployment, with `color = "green"`, `month = 11`, and `year = 2020` to get the number of rows processed

> Answer: 88,605

## Question 5

* Enabled notification for all successful flow using `Notification` menu in Prefect UI and manually triggered custom flow for green taxi data for April 2019
* Following is the notification from slack channel `#testing-notifications`

```
Prefect Notifications
APP  9:51 PM
Prefect flow run notification
Flow run etl-web-to-gcs/spiked-chachalaca entered state Completed at 2023-02-04T21:51:31.642608+00:00.
Flow ID: a5962cf3-e379-4eba-b2a0-d3bd4a32058f
Flow run ID: 8bd560c7-74f9-4890-8fc8-15b4c46fbe18
Flow run URL: http://127.0.0.1:4200/flow-runs/flow-run/8bd560c7-74f9-4890-8fc8-15b4c46fbe18
State message: All states completed.
Prefect NotificationsPrefect Notifications | Today at 9:51 PM
```

> Answer: 514,392

## Question 6

* Created a block using Prefect UI to find the placeholder size Prefect uses to display the hidden password

> Answer: 8