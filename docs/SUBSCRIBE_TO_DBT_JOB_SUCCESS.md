# Subscribe to dbt job success

This guide is for a US Tech team whose app should start work when the data team's dbt build
finishes, such as a sync that reads a table the build writes. You add one Pub/Sub subscription in
your own GCP project through cru-terraform. Adding it changes nothing on the data team's side, and
you own the subscription, how it delivers, and its alerts.

## How it works

Each time a dbt Cloud job in the data team's account succeeds, DOT's `dbt-webhook` function publishes
one message to this topic:

```
projects/cru-data-orchestration-prod/topics/dbt-job-completed
```

- Every job is included, so filter your subscription to the one job you need.
- Only successful runs are published. A failed or cancelled run sends nothing. When DOT's automatic
  retry reruns a failed job and the rerun succeeds, that rerun is published with its own run id.
- There is one topic, in prod. There is no stage topic.
- A message means your job succeeded. Other runs can also rewrite your tables without sending you a
  message, such as the data team's deploy job after a change merges. Your next message comes with
  your own job's next run.

## 1. Read from your own `ext_` dataset

Subscribe only to the dbt job that builds an `ext_` dataset made for your app. Its tables are
versioned, so a breaking change comes as a new version next to the one you use instead of changing
it under you. And no other app reads that dataset. Together, that puts your team in complete control
of any change to the data your app depends on.

If your app doesn't have an `ext_` dataset yet, please create one in dse-dbt, built by its own dbt
job. Then request read access for your app's service account through datahelp@cru.org.

## 2. Find the job id

1. In the BigQuery console, open a table in your `ext_` dataset and look at **Details → Labels**.
   The `dbt_system_to_build` label names the system whose dbt job builds it.
2. In [`jobs.yml`](https://github.com/CruGlobal/dse-dbt-jobs-as-code/blob/master/jobs.yml), search
   for `DBT_SYSTEM_TO_BUILD: <that system>`. The whole value must match: `netsuite` also finds
   `netsuite_for_mpdx`. Of the jobs that match, pick the prod job: the one whose `<<:` line includes
   `*prod` (not `*beta_prod`). Its `linked_id` is the dbt job id.

If more than one prod job matches, or you can't open `jobs.yml`, ask through datahelp@cru.org.

## 3. Add the subscription

One subscription per dbt job, in your app's prod directory, added through cru-terraform. Atlantis
already has the rights to attach a subscription to this topic, so the data team doesn't need to
grant anything.

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

Three things to know about this resource:

- **The filter can't be edited.** Changing it replaces the subscription, and any message waiting at
  that moment is lost. For a second dbt job, add a second subscription.
- **Set `ttl = ""`.** Otherwise Google deletes a subscription after 31 days with no activity.
- **Messages wait up to 7 days** (the default retention) and are retried with backoff until your
  endpoint accepts them.

Besides the subscription, your directory needs the pieces below. The `workvivo-sync` app in
cru-terraform, `applications/workvivo-sync/prod/`, has a working copy of each.

- **The Pub/Sub API.** `cloudrun.tf` turns on `pubsub.googleapis.com` in `enabled_services`. The
  Cru Cloud Run module doesn't turn it on by itself.
- **A push service account, and an endpoint the public can't call.** `cloudrun.tf` makes the
  account (`invoker_service_accounts`) and gives it `roles/run.invoker` on a service whose type
  isn't `app` (`invokers`). In workvivo-sync that is its only service, of type `trigger`. Both
  inputs need the Cloud Run module's `v2-pipeline` ref, which workvivo-sync uses. Don't push to an
  `app` service: unless `load_balancer_strategy` is `"none"`, the module lets anyone call it, so
  nothing would check the token. If your endpoint is public or not on Cloud Run, check the token
  in your code: its signature, its `email` (your push service account) and its `aud` (your
  audience).
- **Pub/Sub's own service account, allowed to sign tokens as your push service account.**
  `failure-route.tf` makes sure it exists (`google_project_service_identity`, which needs
  `google-beta` in `required_providers`, as workvivo-sync's `terraform.tf` has), then gives it
  `roles/iam.serviceAccountTokenCreator` on the push service account.
- **Alerts.** `datadog.tf` has the first of the two alerts in section 6.

## 4. What you receive

Each message has three attributes, all strings: `job_id`, `run_status` and `environment_id`. Your
filter uses them.

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

- With `no_wrapper` and `write_metadata = false`, as above, the POST body is just that JSON and the
  attributes aren't sent. Read the same values from the JSON. Without `no_wrapper`, the body is the
  standard Pub/Sub envelope: the JSON is base64 in `message.data`, and the attributes are in
  `message.attributes`.
- Ids are strings of digits. `run_status_code` is a number. `run_status_humanized` is dbt's free
  text and is empty in practice, so don't rely on it.
- Your job's `run_id` goes up with each new run, in big jumps. That is what the data team has seen;
  dbt doesn't promise it. All jobs share one run id sequence, and a run gets its id when it is
  queued, not when it ends.
- Ignore fields you don't use. New ones may be added.

## 5. Rules for your endpoint

- **Start the work, then answer.** Start slow work somewhere else, such as a Cloud Run job, without
  waiting for it to finish. Give the start a time limit well under the ack deadline (60 seconds
  above); workvivo-sync uses 45. Once it has started, answer with a success code: 102, 200, 201,
  202 or 204. If it fails or runs out of time, answer with an error so Pub/Sub sends the message
  again. Pub/Sub doesn't resend a message you answered with a success code, so a build is lost if
  you answer first and the start then fails. Answer messages you skip with a success code too.
- **Expect repeats and out-of-order delivery.** A repeat carries the same `run_id`. Skip a `run_id`
  you already started work for. If you keep the highest `run_id` you've handled, keep one per
  `job_id` and compare them as numbers, not text. If your job is still running from an older build
  when a newer `run_id` arrives, don't just drop it, or you stay a build behind until the next one:
  queue one more run.
- **A `run_id` of `"0"` is a hand-made test.** Answer it with a success code and skip it. Use `"0"`
  for your own hand-made tests too: a made-up large `run_id` makes the highest-`run_id` rule above
  ignore every real build after it, and no alert notices. To send you a real one, the data team
  re-runs the dbt job.
- **Keep secrets out of the push URL.** It shows up in Terraform plans.

## 6. Alerts

The data team doesn't watch your subscription, so add your own alerts. Add both of these.

- **A message waiting too long:** `gcp.pubsub.subscription.oldest_unacked_message_age` for your
  subscription, as in the workvivo-sync example. It catches an endpoint that is down or refusing.
- **No message accepted for too long.** This catches what the first alert can't, because then
  nothing is waiting: the dbt job stops succeeding, a job it waits on fails (your job may run only
  after another data-team job succeeds), the job is rebuilt with a new id, or the topic is replaced.
  Use a `sum` query with `.as_count()`, and set `on_missing_data = "default"` and
  `require_full_window = false`:

  ```
  sum(last_<N>h):sum:gcp.pubsub.subscription.push_request_count{project_id:<your project>,subscription_id:<your subscription>,response_class:ack}.as_count() < 1
  ```

  With no accepted push, the metric reports no points at all. `on_missing_data = "default"` counts
  that as zero only for a sum or count query, so without both the monitor never alerts. The data
  team's `dbt_job_completed_push_missing` monitor in cru-terraform
  `applications/data-warehouse/dot/prod/datadog.tf` has the full settings. Pick `<N>` a little
  longer than the job's longest normal gap: ask datahelp@cru.org how often the job runs.

If an alert points at the dbt side, the data team usually already knows. Ask datahelp@cru.org for
status.

## 7. Check that it works

After your job's next successful run, graph `gcp.pubsub.subscription.push_request_count` for your
subscription, split by `response_class`, and look for an `ack`. Until that first `ack`, the
no-message alert can't fire, so this check is the only one. If nothing arrives within the job's
normal gap, check the job id in your filter. If pushes arrive but not as `ack`, split by
`response_code`: a 401 or 403 points at the token setup in section 3.

To test before the next real build, ask datahelp@cru.org for a test message. Its `run_id` is
`"0"`, so your endpoint skips it: it proves delivery and the token, not the start of your work.

## If the topic changes

Before renaming the topic or changing the message, the data team lists the topic's subscriptions
and tells the teams that own them. You don't need to sign up, but telling the data team
(datahelp@cru.org) which jobs you listen to means they can warn you when a job is rebuilt and gets a
new id.

If the topic is ever deleted and made again, your subscription stops getting messages until it is
made again too. Your no-message alert catches this.

An endpoint that can't own a subscription (for example, one outside GCP) can ask the data team for
an entry on its push list instead. See
[dbt → outside HTTPS endpoint](ARCHITECTURE.md#dbt--outside-https-endpoint-pubsub-push-no-workflow).
