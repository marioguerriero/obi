# OBI API

This is a WIP component. At the moment it is only useful to get status and general
information about submitted jobs. For example, it is used by the client if a job
is submitted in `wait` mode, returning only when the job is completed.

It runs a [GIN]{https://github.com/gin-gonic/gin} web server, returning a json with
all the information about the requested job. The API endpoint, only in this specific
case, is `https://<service-ip>/<helm-chart-name>/api/jobs?jobid=<job-id>`
The `job-id` value is returned upon job submission by the client.