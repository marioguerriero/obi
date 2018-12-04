# OBI Client

The client accessible through the `obi-submit.go` file,
is meant for end users to allow the submission of jobs to the deployed OBI
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
from the `/etc/obi/credentials`, where the username and password are saved in clear text. If the `--localcreds` flag is omitted, the CLI will ask the user to insert valid credentials before being able to submit a job.

If the `-w` flag is passed, the client will enter in "wait" mode, not returning
until the submitted job is maked by OBI as either "completed" of "failed".
