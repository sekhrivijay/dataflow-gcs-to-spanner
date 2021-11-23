export JOB_NAME="gcs-to-pubsub-`date +%Y%m%d-%H%M%S-%N`"
export TEMPLATE="${BUCKET_NAME}/templates/GCSToPubSubStream.json"

gcloud beta dataflow jobs run ${JOB_NAME} \
--gcs-location=${TEMPLATE} \
--enable-streaming-engine \
--num-workers 10 \
--staging-location ${TEMP_LOCATION} \
--region=us-central1 \
--worker-machine-type=e2-standard-4 \
--additional-experiments=enable_streaming_engine \
--parameters inputFilePattern=${INPUT_FILE_PATTERN},outputTopic="projects/sekhrivijayrcs1/topics/dataflow-gcs-to-spanner"

# --experiments=enable_streaming_engine

