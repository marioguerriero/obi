# OBI (Objectively Better Infrastructure)

[![Build Status](https://travis-ci.com/deliveryhero/obi.svg?token=qEew79ijmfpcMwUpuvpL&branch=master)](https://travis-ci.com/deliveryhero/obi) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

### Simplified batch data processing platform for Google Cloud Dataproc

OBI is a project from Delivery Hero's Data Insight team which represents an
attempt to optimize clusters resource utilization in order to limit their
operational costs. 

- [Objectives](#objectives)
- [Code structure](#code-structure)
- [Architecture](#architecture)
    - [The workflow](#obi-workflow)
- [Helm chart](#helm-chart)
- [Usage](#usage)
- [Contributions](#contributions)
    - [Integrate OBI in your cloud infrastructure](#integrate-obi-in-your-cloud-infrastracture)
    - [Building](#building)

## Objectives

 - Optimize resource usage for low dimensional topologies (e.g., Borg/k8s was
   developed for contexts with vast amount of resources).
 - Delegate the entire value chain of data analysis to the end user removing the
   need for Platform Ops support.
 - Support automation of Data Operations: a user who wants run data analytics
   applications should not be bothered by system details, such as how to
   configure the amount of RAM a Spark Executor should use, how many cores are
   available in the system or even how many worker nodes should be used to meet
   an execution deadline.

## Code Structure

 - `assets` generic assets e.g. images used in the code
 - `chart` OBI's Helm chart for easier Kubernetes deployments
 - `client` the CLI for the final user to allow him to submit his jobs to OBI
 - `Dockerfiles` a collection of Docker images we used to speed-up our components building process
 - `examples` contains example YAML files to showcase how a system administrator
   can attach a cloud computing platform with an OBI deployment
 - `master` the main component which cares about scheduling and autoscaling
 - `predictor` web server listening for requests to the predictive component
 - `predictor/predictors` machine learning models used to provide predictions
   different from job duration
 - `proto` Google Proto Buffer files used to generate RPC communication
   interfaces between components
 - `web` web server exposing an API and a frontend for accessing jobs and clusters managed by OBI

## Architecture

OBI's architecture was designed to be a microservice architecture. In the below
figure, each block but "DATAPROC" are supposed to be separated microservices,
communicating between each other in different ways, depending on the needs. For
example, given the requirement of high performance communication between the
clusters master node and the heartbeat component, those communication is handled
through Google's `protobuf` schemas. All the communication between components
happens trhough RPC while the API component can be accessed through HTTP
requests.

![alt text](assets/obi-architecture.png "OBI Architecture")

### OBI Workflow

OBI is meant to provide an abstraction layer between the user and the cluster
infrastructure, in order to simplify the job submission process and to optimize
operational costs.

OBI jobs may have different priority levels. If a job having maximum priority is
submitted, OBI will create a cluster which will be exclusivelly allocated to
that job. Otherwise, if a job with lower priority is submitted, OBI will try to
pack it with similar jobs in bins, following a certain policy.

The scheduling policy and the priority levels are configurable through the OBI
deployment YAML file. For more information about how to do that, see the README
of the master component.

After jobs are scheduled and one or more clusters are allocated, each cluster is
assigned with an autoscaler routine. This routine will monitor the cluster
status through the heartbeat the OBI Master receives from them. Those heartbeats
contain information about the resources utilization in the cluster which are
used by the autoscaler to decide whether to add (or remove) nodes.

The goal of the autoscaler is to make the cluster working with the least
possible amount of nodes at each time step. Indeed, each cluster created by OBI
starts with the minimum possible amount of nodes, which are dynamically adjusted
according to the needs of the running jobs.

## Helm Chart

In order to make the Kubernetes deployment process easier, we have also provided
an Helm chart available in the `chart` folder.

Open `chart/values.yaml` and fill in all the empty fields. In the `secrets` folder you
have to place two files named `dataproc-sa` and `storage-sa`: they are the service
accounts to use these services inside your Google Cloud Project. Once the 
configuration is completed, just deploy on your Kubernetes cluster with:

```bash
$ helm install chart
```

## Usage
Once an OBI instance is deployed, jobs can be submitted to it. In order to do
that you need to compile the OBI submitter client:

```bash
$ cd client/
$ go get .
$ go build .
```

This process requires you to install Go lang compiler and will produce you a
`client` executable as output. Jobs can be submitted using the following
command:

```bash
$ ./client -f JOB_SCRIPT_PATH -t PySpark -i OBI_DEPLOYMENT_NAME -p PRIORITY -- EXE_ARGS
```

The executable path be either a Google Cloud Storage URI or local executable.
The `-t` option defines the type of the job the user is submitting and, so far,
only PySpark is supported. The `OBI_DEPLOYMENT_NAME` is the chart name used
in the Helm install.

When submitting a job, the user is asked for username and password. Those values
should be written by the system administrator  in the `Users` table of the
PostgreSQL. OBI leverages [Stolon](https://github.com/sorintlab/stolon) in order
to provide HA for the dmbs. The administrator can access the dbms with the following
commands, using the `superuserPassword` specified in `values.yaml`:
```bash
$ kubectl exec -it <pod-name-stolon-proxy> bash -n <namespace>
$ psql --host <helm-chart-name>-stolon-proxy --port 5432 postgres -U stolon -W
```
database which is used by OBI to store its state.

For more info about configuration and customization, see the README 
of each component (master, predictor, api).

## Contributions

### Integrate OBI in your cloud infrastracture

OBI supports only Dataproc as a cloud computing service for now. However, it is
easily extensible to support new platforms by simply adding new code for it. In
fact, you would need to just add the code for you platform under the
`master/platforms` folder. In this code you should provide your own struct
extending `model.ClusterBase` and implementing `model.ClusterBaseInterface`.

As a reference, you can have a look at the existing Dataproc implementation
available in `master/platforms/dataproc.go`.

### Building

The first step to compile OBI is to generate the `protobuf` code required for
RPC communication between the components. Both
[protobuf](https://developers.google.com/protocol-buffers/) and
[gRPC](https://grpc.io) are strict requirements for this process. Keep in mind
that OBI is written mainly in Go with some parts in Python so the dependencies
have to be satisfied for both languages.

Install protobuf following the [instructions](https://github.com/protocolbuffers/protobufs) for your specific environment. 

To generate the `protobuf` code we provide a script, `proto-gen.sh` which will
take care of all the development process.

```bash
$ ./proto-gen.sh
```

After the `protobuf` files are generated, for each component you can find a
`Dockerfile` which can be used to create images from them to be deployed
anywhere.

In order to rebuild the docker image of the modules:
```bash
$ cd <module-name>
$ docker build -t <registry-name>:<tag> .
$ docker push -t <registry-name>:<tag>
```

At this point you should simply specify the new image in the 'values.yaml' file 
for the Helm chart and install again.

Before building the master image, however, you will need to generate SSL/TLS certificates
to be used by the gRPC server side communication encryption. In particular, you need
to generate a a private RSA key to sign and authenticate the public key and a
self-signed X.509 public keys for distribution. Those two keys have to be named
`server.key` and `server.crt` respectivevly and they have to be placed under the
`master/` folder. For more details on how to generate those two keys you can have a
look [here](https://bbengfort.github.io/programmer/2017/03/03/secure-grpc.html).

**IMPORTANT**: because of the fact that we do not have a certificate signed by a trusted certificate authority, at the moment
the client skips the verification of the certificate's trustworthy ([here](https://github.com/deliveryhero/obi/blob/master/client/obi-submit.go#L94)). However, if the user can provide a certificate coming from a trusted certificate authority he can also drop the skip performed by the client [here](https://github.com/deliveryhero/obi/blob/master/client/obi-submit.go#L94).
