package com.google.cloud.teleport.templates;

import org.apache.avro.Schema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.DefaultRetryStrategy;
import org.apache.beam.sdk.io.jdbc.JdbcIO.RetryConfiguration;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.joda.time.Duration;
import org.apache.beam.sdk.options.Default;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubSubToPostgresV2 {
    private static final Logger LOG = LoggerFactory.getLogger(PubSubToPostgresV2.class);

    // public interface Options extends StreamingOptions {
    public interface Options extends PubsubOptions {
        @Description("The Cloud Pub/Sub subscription to consume from. " + "The name should be in the format of "
                + "projects/<project-id>/subscriptions/<subscription-name>.")
        ValueProvider<String> getInputSubscription();

        void setInputSubscription(ValueProvider<String> value);

        @Description("The connection url  of the postgres database in this format dbc:postgresql:///mydb?cloudSqlInstance=project:region:db-instance&socketFactory=com.google.cloud.sql.postgres.SocketFactory")
        @Required
        ValueProvider<String> getDatabaseUrl();

        void setDatabaseUrl(ValueProvider<String> value);

        @Description("The name of the postgres jdbc driver")
        @Required
        @Default.String("org.postgresql.Driver")
        ValueProvider<String> getDatabaseDriver();

        void setDatabaseDriver(ValueProvider<String> value);

        @Description("The user name of the postgres database")
        @Required
        ValueProvider<String> getUserName();

        void setUserName(ValueProvider<String> value);

        @Description("The password of the postgres database")
        @Required
        ValueProvider<String> getPassword();

        void setPassword(ValueProvider<String> value);
    }

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        options.setStreaming(true);
        // options.setPubsubRootUrl("http://localhost:8085");
        run(options);
    }

    public static PipelineResult run(Options options) {
        Schema avroSchema = null;
        try {
            avroSchema = new Schema.Parser().parse(
                    PubSubToPostgresV2.class.getClassLoader()
                            .getResourceAsStream("schema/avro/pubsub.avsc"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply("Read PubSub Events",
                PubsubIO.readAvroGenericRecords(avroSchema).fromSubscription(options.getInputSubscription()))
                // .apply("Materialize input", Reshuffle.viaRandomKey())
                // Finally write to postgres
                .apply("Write to Postgresql", JdbcIO.<GenericRecord>write()
                        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                                .create(options.getDatabaseDriver(), options.getDatabaseUrl())
                                .withUsername(options.getUserName())
                                .withPassword(options.getPassword()))
                        .withRetryStrategy(new DefaultRetryStrategy())
                        .withRetryConfiguration(RetryConfiguration.create(5, null, Duration.standardSeconds(5)))
                        .withStatement("insert into Person values(?, ?)")
                        .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<GenericRecord>() {
                            public void setParameters(GenericRecord elements, PreparedStatement query)
                                    throws SQLException {
                                query.setLong(1, (Long)(elements.get("key1")));
                                query.setString(2, String.valueOf(elements.get("key2")));
                            }
                        }));

        // Execute the pipeline and return the result.
        return pipeline.run();
    }

}
