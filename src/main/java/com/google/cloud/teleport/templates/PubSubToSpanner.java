package com.google.cloud.teleport.templates;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import com.google.cloud.spanner.Value;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.gcp.spanner.LocalSpannerIO;
import com.google.cloud.spanner.Mutation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;

public class PubSubToSpanner {
    public interface Options extends PipelineOptions, StreamingOptions {
        @Description("The Cloud Pub/Sub subscription to consume from. " + "The name should be in the format of "
                + "projects/<project-id>/subscriptions/<subscription-name>.")
        ValueProvider<String> getInputSubscription();

        void setInputSubscription(ValueProvider<String> value);

        @Description("The name of the spanner instance ID")
        @Required
        ValueProvider<String> getInstanceId();

        void setInstanceId(ValueProvider<String> value);

        @Description("The name of the spanner database ID")
        @Required
        ValueProvider<String> getDatabaseId();

        void setDatabaseId(ValueProvider<String> value);

        @Description("Spanner host")
        @Default.String("https://batch-spanner.googleapis.com")
        ValueProvider<String> getSpannerHost();

        void setSpannerHost(ValueProvider<String> value);

        @Description("The name of the spanner project ID")
        @Required
        ValueProvider<String> getSpannerProjectId();

        void setSpannerProjectId(ValueProvider<String> value);

    }

    static class ParseEntity extends DoFn<String, df_test_table> {
        private static final Logger LOG = LoggerFactory.getLogger(ParseEntity.class);
        static final String DELIMITER = ",";

        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] columns = c.element().split(DELIMITER);
            try {
                df_test_table entity = new df_test_table();
                entity.STORE_NO = Long.parseLong(columns[0].trim());
                entity.COM_CD_y = Long.parseLong(columns[1].trim());
                entity.CON_UPC_NO_Y = Long.parseLong(columns[2].trim());
                entity.CAS_UPC_NO = Long.parseLong(columns[3].trim());
                entity.CAS_DSC_TX = columns[4].trim();
                entity.SHF_ALC_QY = columns[5].trim();
                entity.SHF_NO = Long.parseLong(columns[6].trim());
                entity.SHF_MIN_QY = Long.parseLong(columns[7].trim());
                entity.AIL_ORN_CD = columns[8].trim();
                entity.AIL_NO = Long.parseLong(columns[9].trim());
                entity.AIL_LOC_CD = columns[10].trim();

                c.output(entity);
            } catch (ArrayIndexOutOfBoundsException | NumberFormatException e) {
                LOG.info("ParseSinger: parse error on '" + c.element() + "': " + e.getMessage());
            }
        }
    }

    @DefaultCoder(AvroCoder.class)
    static class df_test_table {
        long STORE_NO;
        long COM_CD_y;
        long CON_UPC_NO_Y;
        long CAS_UPC_NO;
        String CAS_DSC_TX;
        String SHF_ALC_QY;
        long SHF_NO;
        long SHF_MIN_QY;
        String AIL_ORN_CD;
        long AIL_NO;
        String AIL_LOC_CD;
    }

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        options.setStreaming(true);
        run(options);
    }

    public static PipelineResult run(Options options) {
        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);
        
        SpannerConfig spannerConfig = SpannerConfig.create().withHost(options.getSpannerHost())
        .withProjectId(options.getSpannerProjectId()).withInstanceId(options.getInstanceId())
        .withDatabaseId(options.getDatabaseId());


        pipeline.apply("Read PubSub Events", PubsubIO.readStrings().fromSubscription(options.getInputSubscription()))

                .apply("ParseEntity", ParDo.of(new ParseEntity()))
                .apply("CreateEntityMutation", ParDo.of(new DoFn<df_test_table, Mutation>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        df_test_table entity = c.element();
                        c.output(Mutation.newInsertOrUpdateBuilder("df_test_table2").set("STORE_NO").to(entity.STORE_NO)
                                .set("COM_CD_y").to(entity.COM_CD_y).set("CON_UPC_NO_Y").to(entity.CON_UPC_NO_Y)
                                .set("CAS_UPC_NO").to(entity.CAS_UPC_NO).set("CAS_DSC_TX").to(entity.CAS_DSC_TX)
                                .set("SHF_ALC_QY").to(entity.SHF_ALC_QY).set("SHF_NO").to(entity.SHF_NO)
                                .set("SHF_MIN_QY").to(entity.SHF_MIN_QY).set("AIL_ORN_CD").to(entity.AIL_ORN_CD)
                                .set("AIL_NO").to(entity.AIL_NO).set("AIL_LOC_CD").to(entity.AIL_LOC_CD).set("t")
                                .to(Value.COMMIT_TIMESTAMP).build());
                    }
                }))
                // Finally write the Mutations to Spanner
                .apply("WriteEntities", LocalSpannerIO.write().withSpannerConfig(spannerConfig).withMaxNumRows(1));

        // Execute the pipeline and return the result.
        return pipeline.run();
    }

}
