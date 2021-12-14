export PROJECT=sekhrivijayrcs1
export BUCKET_NAME="gs://sekhrivijay-rcs-dataflow-test1"
export TEMP_LOCATION="${BUCKET_NAME}/temp"
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=CnnaoB5i0bO4ml5t

export POSTGRES_DATABASE_INSTANCE="sekhrivijayrcs1:us-central1:rcs-instance"
export POSTGRES_DATABASE_NAME=mytestdb
export POSTGRES_CONNECTION_URL="jdbc:postgresql:///${POSTGRES_DATABASE_NAME}?cloudSqlInstance=${POSTGRES_DATABASE_INSTANCE}&socketFactory=com.google.cloud.sql.postgres.SocketFactory"
export GOOGLE_APPLICATION_CREDENTIALS=sa.json
#export INPUT_SUB="projects/sekhrivijayrcs1/subscriptions/pubsub-avro-topic-sub"
export INPUT_SUB="projects/sekhrivijayrcs1/subscriptions/debug-sub"
gcloud config set project ${PROJECT}
export TEMPLATE="${BUCKET_NAME}/templates/PubSubAvroToPostgresStream.json"
