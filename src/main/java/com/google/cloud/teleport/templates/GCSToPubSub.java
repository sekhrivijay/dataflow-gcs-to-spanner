package com.google.cloud.teleport.templates;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Watch;
import org.joda.time.Duration;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;

public class GCSToPubSub {
  private static final Duration DEFAULT_POLL_INTERVAL = Duration.standardSeconds(10);

  public interface Options extends PipelineOptions {
    @Description("The file pattern to read records from (e.g. gs://bucket/file-*.csv)")
    @Required
    ValueProvider<String> getInputFilePattern();

    void setInputFilePattern(ValueProvider<String> value);

    @Description(
        "The name of the topic which data should be published to. "
            + "The name should be in the format of projects/<project-id>/topics/<topic-name>.")
    @Required
    ValueProvider<String> getOutputTopic();

    void setOutputTopic(ValueProvider<String> value);
  }


  public static void main(String[] args) {

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    run(options);
  }

  public static PipelineResult run(Options options) {

    Pipeline pipeline = Pipeline.create(options);


    pipeline
        .apply(
            "Read Text Data",
            TextIO.read()
                .from(options.getInputFilePattern())
                .watchForNewFiles(DEFAULT_POLL_INTERVAL, Watch.Growth.never()))
        .apply("Write to PubSub", PubsubIO.writeStrings().to(options.getOutputTopic()));

    return pipeline.run();
  }
}
