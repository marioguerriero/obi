# OBI Web

This component provides a web interface for real-time monitoring of OBI internal state.
The source code is divided mainly into two parts:

 - `backend` exposing OBI's internal state through an HTTP API
 - `frontend` providing the actual web interface

# API

## Open Endpoints

Open endpoints require no Authentication.

* [Login](#login) : `POST /api/login/`

## Endpoints that require Authentication

* [Clusters](#clusters) : `GET /api/clusters`
* [Cluster](#cluster) : `GET /api/cluster/:name`
* [Jobs](#jobs) : `GET /api/jobs`
* [Job](#job) : `GET /api/job/:id`
* [User](#user) : `GET /api/user/:id`

Authentication is always done through using a Bearer token in the requests header. Specifically, the token to be used is the one returned by the `/api/login` endpoint on successful authentication.

### Login

Used to collect a Token for a registered User.

**URL** : `/api/login/`

**Method** : `POST`

**Auth required** : NO

**Data constraints**

```json
{
    "username": "[valid email address]",
    "password": "[password in plain text]"
}
```

**Data example**

```json
{
    "username": "iloveauth@example.com",
    "password": "abcd1234"
}
```

#### Success Response

**Code** : `200 OK`

**Content example**

```json
TOKEN
```



### Clusters

Used to collect a collection of clusters. If no query parameters are specified all the clusters will be returned.

For more specific cluster data examples look below.

**URL** : `/api/clusters[?status=STATUS&name=CLUSTER_NAME]`

**Method** : `GET`

**Auth required** : YES

#### Success Response

**Code** : `200 OK`

**Content example**

```json
[
  CLUSTER_1,
  ...
  CLUSTER_n
]
```




### Cluster

Used to collect a specific cluster details.

**URL** : `/api/cluster/:name`

**Method** : `GET`

**Auth required** : YES

#### Success Response

**Code** : `200 OK`

**Content example**

```json
{
    "name": "abcdefghi",
    "platform": "dataproc",
    "status": "running",
    "creationtimestamp": "2018-12-03 15:26:18.993021",
    "cost": 1.23,
    "lastupdatetimestamp": "2018-12-03 15:26:21.715841",
    "assignedjobs": 12
}
```





### Jobs

Used to collect a collection of jobs. If no query parameters are specified all the jobs will be returned.

For more specific job data examples look below.

**URL** : `/api/jobs[?status=STATUS&cluster=CLUSTER_NAME]`

**Method** : `GET`

**Auth required** : YES

#### Success Response

**Code** : `200 OK`

**Content example**

```json
[
  JOB_1,
  ...
  JOB_n
]
```




### Job

Used to collect a specific job details.

**URL** : `/api/job/:id`

**Method** : `GET`

**Auth required** : YES

#### Success Response

**Code** : `200 OK`

**Content example**

```json
{
    "id": 123,
    "clustername": "abcdefghi",
    "status": "running",
    "author": 1,
    "creationtimestamp": "2018-12-03 15:26:18.993021",
    "lastupdatetimestamp": "2018-12-03 15:26:21.715841",
    "executablepath": "gs://fancy-bucket/hello.py",
    "type": "pyspark",
    "priority": 2,
    "predictedduration": 12345,
    "failureprobability": 0.5,
    "arguments": "--hello",
    "platformdependentid": "1a2s3d4f5g"
}
```



### User

Used to obtain the email of a user given his ID.

**URL** : `/api/user/:id`

**Method** : `GET`

**Auth required** : YES

#### Success Response

**Code** : `200 OK`

**Content example**

```json
{
    "email": "iloveauth@example.com",
}
```
