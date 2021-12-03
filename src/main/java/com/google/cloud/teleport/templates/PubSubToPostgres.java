package com.google.cloud.teleport.templates;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;

import org.apache.beam.sdk.options.Description;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.beam.sdk.options.Default;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubSubToPostgres {
    private static final Logger LOG = LoggerFactory.getLogger(PubSubToPostgres.class);

    public interface Options extends StreamingOptions {
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
        run(options);
    }

    public static PipelineResult run(Options options) {
        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("Read PubSub Events", PubsubIO.readStrings().fromSubscription(options.getInputSubscription()))
                .apply("Materialize input", Reshuffle.viaRandomKey())
                .apply("Split into tokens", ParDo.of(new DoFn<String, String[]>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        c.output(c.element().split(","));
                    }
                }))

                // Finally write to postgres
                .apply("Write to Postgresql", JdbcIO.<String[]>write()
                        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                                .create(options.getDatabaseDriver(), options.getDatabaseUrl())
                                .withUsername(options.getUserName())
                                .withPassword(options.getPassword())

                        )
                        .withStatement("insert into Person values(?, ?)")
                        .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<String[]>() {
                            public void setParameters(String[] elements, PreparedStatement query)
                                    throws SQLException {
                                query.setInt(1, Integer.parseInt(elements[0]));
                                query.setString(2, elements[1]);
                            }
                        }));

        // Execute the pipeline and return the result.
        return pipeline.run();
    }

}
