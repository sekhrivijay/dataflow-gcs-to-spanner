export JOB_NAME="gcs-to-spanner-`date +%Y%m%d-%H%M%S-%N`"


gcloud beta dataflow jobs run ${JOB_NAME} \
--gcs-location=${TEMPLATE} \
--enable-streaming-engine \
--num-workers 5 \
--staging-location ${TEMP_LOCATION} \
--region=us-central1 \
--worker-machine-type=n2-standard-8 \
--additional-experiments=enable_streaming_engine,min_num_workers=4,use_runner_v2,autoscaling_algorithm=THROUGHPUT_BASED \
--parameters inputFilePattern=${INPUT_FILE_PATTERN},instanceId=${SPANNER_INSTANCE_ID},databaseId=${SPANNER_DATABASE_ID},spannerProjectId=${SPANNER_PROJECT_ID}

#--number-of-worker-harness-threads=500 \
# --experiments=enable_streaming_engine
#--worker-machine-type=e2-standard-4 \

