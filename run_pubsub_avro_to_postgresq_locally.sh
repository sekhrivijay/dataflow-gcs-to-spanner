mvn clean
mvn compile exec:java \
-Dexec.mainClass=com.google.cloud.teleport.templates.PubSubToPostgresV2 \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--project=${PROJECT} \
--stagingLocation=${BUCKET_NAME}/staging \
--tempLocation=${TEMP_LOCATION} \
--templateLocation=${TEMPLATE} \
--databaseUrl=${POSTGRES_CONNECTION_URL} \
--userName=${POSTGRES_USER} \
--password=${POSTGRES_PASSWORD} \
--inputSubscription=${INPUT_SUB}"

#--parameters host=${POSTGRES_HOST_NAME},databaseName=${POSTGRES_DATABASE_NAME},userName=${POSTGRES_USER},password=${POSTGRES_PASSWORD},inputSubscription=${INPUT_SUB}




