# OBI Client

Along with OBI also two separate clients were developed: one for system
administrators and another one for users.

The client for system administrators is written in Python and is accessible
through the `client.py` file from this folder.  Its usage is meant to be as
simple as possible, trying to resemble the CLI syntax of the `kubectl` utility.
It allows a system administrator to deploy a new instance of OBI, according to
the configuration parameters specified in the `obi_client_config.yaml` file.
The client for system administrators was designed to support several deployment
modes, in accordance with OBI "run everywhere" principle. At the moment this
client only supports deployments on Kubernetes (which is actually deprecated in
favor of OBI's Helm chart) and on local system through Docker (this feature is
still a work in progress though).

The creation of an instance through the system administrator client requires
an OBI deployment Yaml file, containing all the specifications for the deployment
the user is trying to create. Assuming that the OBI deployment YAML file is in
`examples/deployment.yaml`, you can deploy a new OBI with the following command:

```bash
$ python3 generic_client.py create infrastructure -f ../examples/deployment.yaml
```

On the other hand, the client accessible through the `obi-submit.go` file,
is meant for end users and is written in Go. Unlike the previous one, the
features of this client are limited to the submission of jobs a given OBI
platform.

After the necessary Protobuf files are generated (see main README for more
details on this), he end-user client can be compiled with:

```
go build .
```

and it can be used to submit a job using the following CLI syntax:

```
./client -f JOB_PATH -t (PySpark) -i OBI_INSTANCE_NAME -p PRIORITY_LEVEL [--localcreds] [-w] -- JOB_ARGS
```

If the `--localcreds` flag is passed, the client will load OBI's credentials
from the `/usr/local/airflow/dags/obi-exec/credentials`. The reason behind the
choice of this path is that we primarly use OBI from Airflow scheduler and,
given our infrastructure, we found it convenient to put the credentials
file in that path. However, it can be easily changed before compiling 
the `obi-submit.go` file. If the `--localcreds` flag is omitted, the CLI
will ask the user to insert valid credentials before being able to submit a job.

If the `-w` flag is passed, the client will enter in "wait" mode, not returning
until the submitted job is maked by OBI as either "completed" of "failed".