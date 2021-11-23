

mvn clean
mvn compile exec:java \
-Dexec.mainClass=com.google.cloud.teleport.templates.PubSubToSpanner \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--project=${PROJECT} \
--stagingLocation=${BUCKET_NAME}/staging \
--tempLocation=${TEMP_LOCATION} \
--templateLocation=${TEMPLATE} \
--runner=DataflowRunner"




