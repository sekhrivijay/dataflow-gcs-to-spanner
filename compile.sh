

mvn clean
mvn compile exec:java \
-Dexec.mainClass=com.google.cloud.teleport.templates.GCSToSpannerStream \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--project=${PROJECT} \
--region=us-central1 \
--stagingLocation=${BUCKET_NAME}/staging \
--tempLocation=${TEMP_LOCATION} \
--templateLocation=${TEMPLATE} \
--runner=DataflowRunner"




