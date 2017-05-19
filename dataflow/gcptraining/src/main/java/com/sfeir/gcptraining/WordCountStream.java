package com.sfeir.gcptraining;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

import com.sfeir.gcptraining.WordCount.CountWords;
import com.sfeir.gcptraining.WordCount.FormatAsTableRow;

public class WordCountStream {
	  public interface WordCountOptions extends PubsubOptions {

		    /**
		     * Set this required option to specify where to write the output.
		     */
		    @Description("Path of the file to write to")
		    @Required
		    String getOutput();
		    void setOutput(String value);

		    /**
		     * Set this required option to specify where to write the output.
		     */
		    @Description("Table for BigQuery : dataset.table")
		    @Required
		    String getTableBQ();
		    void setTableBQ(String value);  

		    /**
		     * Set this required option to specify where to write the output.
		     */
		    @Description("Topic for pubsub")
		    @Default.String("mytopic")
		    String getTopic();
		    void setTopic(String value);  

		    @Description("Table for BigQuery : dataset.table")
			@Default.InstanceFactory(TableNameBQFactory.class)
		    @Required
		    String getTableCompleteBQ();
		    void setTableCompleteBQ(String value);  

		    @Description("Topic for pubsub")
			@Default.InstanceFactory(PubSubFactory.class)
		    String getTopicComplete();
		    void setTopicComplete(String value);  

		    static class TableNameBQFactory implements DefaultValueFactory<String> {
				@Override
				public String create(PipelineOptions options) {
					WordCountOptions woptions = options.as(WordCountOptions.class);
					return woptions.getProject() + ":" + woptions.getTableBQ();
				}
		    }

		    static class PubSubFactory implements DefaultValueFactory<String> {
				@Override
				public String create(PipelineOptions options) {
					WordCountOptions woptions = options.as(WordCountOptions.class);
					return "projects/" + woptions.getProject() + "/topics/" + woptions.getTopic();
				}
		    }
		  }
	  
	  public static void main(String[] args) {
		    WordCountOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
		      .as(WordCountOptions.class);
		    Pipeline p = Pipeline.create(options);

		    PCollection<KV<String, Long>> counts = p.apply("ReadLines PubSub", PubsubIO.readStrings().fromTopic(options.getTopicComplete()))
										    		.apply(Window.<String>into(
											    		    FixedWindows.of(Duration.standardMinutes(1))))
		    										.apply(new CountWords());

		     counts.apply(MapElements.via(new FormatAsTableRow()))
			   	   .apply("WriteCounts in BigQuery", 
			   			 BigQueryIO.writeTableRows()
				   	        .to(options.getTableCompleteBQ())
				   	        .withSchema(WordCount.buildSchema())
				   	        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
		    p.run().waitUntilFinish();
		  }
}
