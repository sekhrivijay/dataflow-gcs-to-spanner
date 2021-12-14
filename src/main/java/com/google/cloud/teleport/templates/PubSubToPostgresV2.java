package com.google.cloud.teleport.templates;


import org.apache.avro.file.DataFileStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.DefaultRetryStrategy;
import org.apache.beam.sdk.io.jdbc.JdbcIO.RetryConfiguration;

import java.io.ByteArrayInputStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.joda.time.Duration;
import org.apache.beam.sdk.options.Default;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubSubToPostgresV2 {
    private static final Logger LOG = LoggerFactory.getLogger(PubSubToPostgresV2.class);


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
    //    Schema avroSchema = null;
    //     try {
    //         avroSchema = new Schema.Parser().parse(
    //                 PubSubToPostgresV2.class.getClassLoader()
    //                         // .getResourceAsStream("schema/avro/pubsub.avsc"));
    //                         .getResourceAsStream("schema/avro/rcs.avsc"));
    //     } catch (IOException e) {
    //         e.printStackTrace();
    //     }
        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply("Read PubSub Events",
                PubsubIO.readMessages().fromSubscription(options.getInputSubscription()))
                //  PubsubIO.readAvroGenericRecords(avroSchema).fromSubscription(options.getInputSubscription()))

                .apply("ParseEntity", ParDo.of(new ParseEntity()))
                // Finally write to postgres
                .apply("Write to Postgresql", JdbcIO.<Example>write()
                        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                                .create(options.getDatabaseDriver(), options.getDatabaseUrl())
                                .withUsername(options.getUserName())
                                .withPassword(options.getPassword()))
                        .withRetryStrategy(new DefaultRetryStrategy())
                        .withRetryConfiguration(RetryConfiguration.create(5, null, Duration.standardSeconds(5)))
                        .withStatement("insert into Person values(?, ?)")
                        .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<Example>() {
                            public void setParameters(Example elements, PreparedStatement query)
                                    throws SQLException {
                                query.setLong(1, elements.uuid);
                                query.setString(2, elements.CommitTimestamp);
                            }
                        }));

        // Execute the pipeline and return the result.
        return pipeline.run();
    }

    @DefaultCoder(AvroCoder.class)
    public static class Example {
        public long uuid;
        public String CommitTimestamp;
    }

    static class ParseEntity extends DoFn<PubsubMessage, Example> {
        private static final Logger LOG = LoggerFactory.getLogger(ParseEntity.class);
            
        @ProcessElement
        public void processElement(ProcessContext c) {
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
            try {
                DataFileStream<GenericRecord> dataFileReader  = new DataFileStream<GenericRecord>(new ByteArrayInputStream(c.element().getPayload()), datumReader);
                while (dataFileReader.hasNext()) {
                    GenericRecord avroRec = dataFileReader.next();
			        LOG.info("avroRec " + avroRec);
                    Example ex = new Example();
                    ex.uuid = (Long)avroRec.get("uuid");
                    ex.CommitTimestamp = String.valueOf(avroRec.get("CommitTimestamp"));
                    c.output(ex);
                }
            } catch (Exception  e) {
                LOG.info("ParseSinger: parse error on '" + c.element() + "': " + e.getMessage());
                LOG.error("Some parse error", e);
            }
        }

    }
}
