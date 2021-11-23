export JOB_NAME="gcs-to-spanner-`date +%Y%m%d-%H%M%S-%N`"


gcloud beta dataflow jobs run ${JOB_NAME} \
--gcs-location=${TEMPLATE} \
--enable-streaming-engine \
--num-workers 10 \
--staging-location ${TEMP_LOCATION} \
--region=us-central1 \
--worker-machine-type=e2-standard-4 \
--additional-experiments=enable_streaming_engine \
--parameters inputFilePattern=${INPUT_FILE_PATTERN},instanceId=${SPANNER_INSTANCE_ID},databaseId=${SPANNER_DATABASE_ID},spannerProjectId=${SPANNER_PROJECT_ID}

# --experiments=enable_streaming_engine

