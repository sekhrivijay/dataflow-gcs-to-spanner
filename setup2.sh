export PROJECT=sekhrivijayrcs1
export BUCKET_NAME="gs://sekhrivijay-rcs-dataflow-test1"
export TEMP_LOCATION="${BUCKET_NAME}/temp"
export INPUT_FILE_PATTERN="gs://sekhrivijayrcs1-input-data/*.csv"
export SPANNER_INSTANCE_ID=test-input-data
export SPANNER_DATABASE_ID=input-db-pubsub
export SPANNER_PROJECT_ID=${PROJECT}
export GOOGLE_APPLICATION_CREDENTIALS=sa.json
gcloud config set project ${PROJECT}
export TEMPLATE="${BUCKET_NAME}/templates/PubSubToSpanner.json"
