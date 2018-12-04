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

After first submission, the credentials could be saved in the system keychain (thanks to [zalando/go-keyring](https://github.com/zalando/go-keyring)). If the `--reset-creds` flag is passed, the local credentials will be deleted.

If the `-w` flag is passed, the client will enter in "wait" mode, not returning
until the submitted job is maked by OBI as either "completed" of "failed".
