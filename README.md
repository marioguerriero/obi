# OBI (Objectively Better Infrastructure)

**DISCLAIMER**: OBI is still WIP and the information in this README is very
likely to change quite often.

OBI is a project from Delivery Hero's Data Insight team which represents
an attempt to optimize clusters resource utilization in order to limit
their operational costs. 

Project goals:

 - Optimize resource usage for low dimensional topologies (e.g., Borg/k8s was developed for 
contexts with vast amount of resources).
 - Delegate the entire value chain of data analysis to the end user removing the need for 
Platform Ops support.
 - Support automation of Data Operations: a user who wants run data analytics applications 
should not be bothered by system details, such as how to configure the amount of RAM a Spark 
Executor should use, how many cores are available in the system or even how many worker 
nodes should be used to meet an execution deadline.


## Code Structure

 - `assets` generic assets e.g. images used in the code
 - `autoscaler` code written for the autoscaler feature
 - `heartbeat` set of scripts and `protobuf` schemas used to run the heartbeat service on clusters master node and to collect them into OBI architecture
 - `model` definition of generic data structure, which define how each cluster should be interfaced
 - `platforms` implementations of the generic interfaces from `model`, capable of extending OBI to each possible cloud computing service
 - `pooling` cluster pooling source code
 - `predictor` web server listening for requests to the predictive component
 - `predictor/models` machine learning models used to provide predictions different from job duration
 - `predictor/profiles` set of pre-trained profiles
 - `utils` set of utility functions which may be used throughout all components

In the above description, by profile we mean a machine learning model specifically trained to predict
job duration for a particular type of workload. They are treated separately with respect to other models
because the profile is something the user has to specify while submitting a job.

We maintain separate profiles for each possible type of workload with the only purpose of
improving our predictive abilities.

Some users may complain about having to specify a "profile" argument while submitting a job. However,
we believe that it is not a hard task for him, as he probably knows the type of workload addressed by
the job he is submitting. 

## Architecture

OBI's architecture was designed to be a microservice architecture. In the
below figure, each block but "DATAPROC API" are supposed to be separated
microservices, communicating between each other in different ways, depending
on the needs. For example, given the requirement of high performance communication
between the clusters master node and the heartbeat component, those communication
is handled through Google's `protobuf` schemas.

![alt text](assets/obi-architecture.png "OBI Architecture")

## Contributions

### Integrate OBI in your infrastructure