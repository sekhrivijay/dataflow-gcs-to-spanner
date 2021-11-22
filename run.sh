export JOB_NAME="gcs-to-spanner-`date +%Y%m%d-%H%M%S-%N`"


gcloud dataflow jobs run ${JOB_NAME} \
--gcs-location=${TEMPLATE} \
--enable-streaming-engine \
--num-workers 10 \
--staging-location ${TEMP_LOCATION} \
--region=us-central1 \
--parameters inputFilePattern=${INPUT_FILE_PATTERN},instanceId=${SPANNER_INSTANCE_ID},databaseId=${SPANNER_DATABASE_ID},spannerProjectId=${SPANNER_PROJECT_ID}

