# Subscribe to dbt job success

For a team whose app should start work when the data team's dbt build finishes, such as a sync that
reads a table the build writes. You add one Pub/Sub subscription in your own GCP project through
cru-terraform. Nothing changes on the data team's side, and you own the subscription, how it
delivers, and its alerts.

## How it works

Each time a dbt Cloud job in the data team's account succeeds, DOT's `dbt-webhook` function publishes
one message to this topic:

```
projects/cru-data-orchestration-prod/topics/dbt-job-completed
```

- Every job is included, so filter your subscription to the one job you need.
- Only successful runs are published. A failed or cancelled run sends nothing. When DOT's automatic
  retry reruns a failed job and the rerun succeeds, that rerun is published with its own run id.

## 1. Find the job id

1. In the BigQuery console, open the table your app reads and look at **Details → Labels**. The
   `dbt_system_to_build` label names the system whose dbt job builds that table.
2. In [`jobs.yml`](https://github.com/CruGlobal/dse-dbt-jobs-as-code/blob/master/jobs.yml), search
   for `DBT_SYSTEM_TO_BUILD: <that system>`. Pick the prod job: the one that uses `*prod`, usually
   named `<system>_prod`. Its `linked_id` is the dbt job id.

Ask the data team (dps-dse@cru.org) instead if you can't see the table's labels or open
`jobs.yml`, the table has no `dbt_system_to_build` label, more than one prod job matches, or the
job hasn't run lately.

## 2. Add the subscription

One subscription per dbt job, in your own project. Add it through cru-terraform: Atlantis already
has the rights to attach a subscription to this topic. If you create it any other way (console,
`gcloud`, another pipeline), ask the data team first for `roles/pubsub.subscriber` on the topic.

A Cloud Run push target looks like this:

```hcl
resource "google_pubsub_subscription" "dbt_job_completed" {
  project              = "<your project>"
  name                 = "dbt-job-completed-<your app>"
  topic                = "projects/cru-data-orchestration-prod/topics/dbt-job-completed"
  filter               = "attributes.job_id = \"<job id>\""
  ack_deadline_seconds = 60

  expiration_policy {
    ttl = ""
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  push_config {
    push_endpoint = "<your service URL>"

    oidc_token {
      service_account_email = "<your push service account>"
      audience              = "<your service URL>"
    }

    no_wrapper {
      write_metadata = false
    }
  }
}
```

A working example is the `workvivo-sync` app in cru-terraform, `applications/workvivo-sync/prod/`:

- `build-trigger.tf`: the subscription.
- `cloudrun.tf`: the push service account (`invoker_service_accounts`) and its `roles/run.invoker`
  on the service (`invokers`).
- `failure-route.tf`: lets Pub/Sub in that project sign tokens as the push service account
  (`roles/iam.serviceAccountTokenCreator` for the Pub/Sub service agent).
- `datadog.tf`: the alert.

- **The filter can't be edited.** Changing it replaces the subscription, and any message waiting at
  that moment is lost. For a second dbt job, add a second subscription.
- **Set `ttl = ""`.** Otherwise Google deletes a subscription after 31 days with no activity.
- **Messages wait up to 7 days** (the default retention) and are retried with backoff until your
  endpoint accepts them.

## 3. What you receive

Each message has three attributes, all strings: `job_id`, `run_status` and `environment_id`.

Its data is this JSON:

```json
{
  "job_id": "<digits>",
  "job_name": "<dbt job name>",
  "run_id": "<digits>",
  "run_status": "Success",
  "run_status_code": 10,
  "run_status_humanized": "<free text from dbt>",
  "environment_id": "<digits>",
  "account_id": "<digits>",
  "event_type": "job.run.completed"
}
```

- With `no_wrapper`, the POST body is that JSON. Without it, the body is the standard Pub/Sub
  envelope: the JSON is base64 in `message.data`, and the attributes are in `message.attributes`.
- Ids are strings of digits. `run_status_code` is a number. `run_status_humanized` is dbt's free
  text and is often empty, so don't rely on it.
- `run_id` goes up with each new run, in big jumps, never by 1. That is what we've seen; dbt doesn't
  promise it.

## 4. Rules for your endpoint

- **Answer fast, to every message.** Return 102, 200, 201, 202 or 204 within the ack deadline (60
  seconds above), including for messages you decide to skip. Anything else, or no answer in time,
  is retried. Do slow work after answering: start a job, don't run the sync inside the request.
- **Expect repeats and out of order delivery.** A repeat carries the same `run_id`. Make a repeat
  harmless, for example by skipping when your job is already running, or by skipping any `run_id`
  at or below the highest one you've handled.
- **A `run_id` of `"0"` is a hand-made test** from the data team, not a build. To send you a real
  one again, the data team re-runs the dbt job.
- **Keep secrets out of the push URL.** It shows up in Terraform plans.

## 5. Alerts

The data team doesn't watch your subscription, so add your own alert. The usual one is a message
waiting too long: `gcp.pubsub.subscription.oldest_unacked_message_age` for your subscription, as in
the workvivo-sync example. That alert can't tell you when the dbt job stops succeeding, since then
nothing is sent. If that matters, also alert when your subscription has accepted no push for longer
than the job's normal gap (`gcp.pubsub.subscription.push_request_count` with `response_class:ack`).
Set `on_missing_data = "default"` on that monitor: with no accepted push, the metric reports no
points at all, and without that setting the monitor never alerts. The data team's
`dbt_job_completed_push_missing` monitor in cru-terraform
`applications/data-warehouse/dot/prod/datadog.tf` is an example.

## 6. Reading the data

Getting told the build finished doesn't grant access to the table. Ask the data team
(dps-dse@cru.org) for read access to the dataset, naming the service account your job runs as.

## If the topic changes

Before renaming the topic or changing the message, the data team checks the topic's subscriptions
and tells the teams that own them. You don't need to sign up, but telling the data team which jobs
you listen to means they can warn you when a job is rebuilt and gets a new id.

An endpoint that can't own a subscription (for example, one outside GCP) can ask the data team for
an entry on its push list instead. See
[dbt → outside HTTPS endpoint](ARCHITECTURE.md#dbt--outside-https-endpoint-pubsub-push-no-workflow).
