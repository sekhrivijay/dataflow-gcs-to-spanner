mvn clean 
mvn compile exec:java \
-Dexec.mainClass=com.google.cloud.teleport.templates.GCSToSpannerStream \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--project=${PROJECT} \
--region=us-central1 \
--stagingLocation=${BUCKET_NAME}/staging1 \
--tempLocation=${TEMP_LOCATION} \
--templateLocation=${TEMPLATE} \
--inputFilePattern=${INPUT_FILE_PATTERN} \
--instanceId=${SPANNER_INSTANCE_ID} \
--databaseId=${SPANNER_DATABASE_ID} \
--spannerProjectId=${SPANNER_PROJECT_ID}"


