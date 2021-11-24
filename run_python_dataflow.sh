export PROJECT_ID=sekhrivijayrcs1
export BUCKET_NAME="gs://sekhrivijay-rcs-dataflow-test1"
export INPUT_FILE_PATTERN="gs://sekhrivijayrcs1-input-data/*.csv"
export GOOGLE_APPLICATION_CREDENTIALS=sa.json
export TOPIC_ID=dataflow-gcs-to-spanner
export TEMP_LOCATION="${BUCKET_NAME}/temp"
python3 GCSToSpannerDataFlow.py \
  --project=$PROJECT_ID \
  --region=us-central1 \
  --worker-machine-type=n2-standard-8 \
  --output_topic=projects/$PROJECT_ID/topics/$TOPIC_ID \
  --input_file_pattern=${INPUT_FILE_PATTERN} \
  --spanner_project_id=${PROJECT_ID} \
  --spanner_instance_id=test-input-data \
  --spanner_database_id=input-db-pubsub \
  --runner=DataflowRunner \
