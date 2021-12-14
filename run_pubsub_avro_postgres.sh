export JOB_NAME="pubsub-avro-to-postgresq`date +%Y%m%d-%H%M%S-%N`"

gcloud dataflow jobs run ${JOB_NAME} \
--gcs-location=${TEMPLATE} \
--enable-streaming-engine \
--num-workers 3 \
--staging-location ${TEMP_LOCATION} \
--region=us-central1 \
--worker-machine-type=e2-standard-4 \
--additional-experiments=enable_streaming_engine,min_num_workers=2,autoscaling_algorithm=THROUGHPUT_BASED \
--parameters databaseUrl=${POSTGRES_CONNECTION_URL},userName=${POSTGRES_USER},password=${POSTGRES_PASSWORD},inputSubscription=${INPUT_SUB}
#--parameters databaseInstance=${POSTGRES_DATABASE_INSTANCE},databaseName=${POSTGRES_DATABASE_NAME},userName=${POSTGRES_USER},password=${POSTGRES_PASSWORD},inputSubscription=${INPUT_SUB}
#--parameters databaseHost=${POSTGRES_HOST_NAME},databaseName=${POSTGRES_DATABASE_NAME},userName=${POSTGRES_USER},password=${POSTGRES_PASSWORD},inputSubscription=${INPUT_SUB}


