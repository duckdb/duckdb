To be able to run the S3 tests locally:

On first run, you have to first install the Minio docker image
run `sudo scripts/install_s3_test_server.sh`

Then to start the Minio docker containers:
run `scripts/run_s3_test_server.sh`
(Do not run this command as root (sudo), or the folders will be created as root and the permissions will be messed up)

Finally to set the environment variables necessary to connect to the Minio instance:
run: `source scripts/set_s3_test_server_variables.sh`
(Run this with `source` so it can actually set the environment variables in your current environment)
