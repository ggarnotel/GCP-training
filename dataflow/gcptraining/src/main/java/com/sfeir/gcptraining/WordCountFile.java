package com.sfeir.gcptraining;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import com.sfeir.gcptraining.WordCount.CountWords;
import com.sfeir.gcptraining.WordCount.FormatAsTableRow;
import com.sfeir.gcptraining.WordCount.FormatAsTextFn;

public class WordCountFile {

	public interface WordCountOptions extends PipelineOptions, BigQueryOptions {
	
		    /**
	     * By default, it's bucket sample
	     */
	    @Description("Path of the file to read from")
	    @Default.String("gs://apache-beam-samples/")
	    String getBucketFile();
	    void setBucketFile(String value);
	
	    /**
	     * By default, this example reads from a public dataset containing the text of
	     * King Lear. Set this option to choose a different input file or glob.
	     */
	    @Description("Path of the file to read from")
	    @Default.String("shakespeare/kinglear.txt")
	    String getInputFile();
	    void setInputFile(String value);
	
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
	
	    @Description("Table for BigQuery : dataset.table")
		@Default.InstanceFactory(TableNameBQFactory.class)
	    @Required
	    String getTableCompleteBQ();
	    void setTableCompleteBQ(String value);  
	
	    static class TableNameBQFactory implements DefaultValueFactory<String> {
			@Override
			public String create(PipelineOptions options) {
				WordCountOptions woptions = options.as(WordCountOptions.class);
				return woptions.getProject() + ":" + woptions.getTableBQ();
			}
	    }
	
	}

	  public static void main(String[] args) {
		    WordCountOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
		      .as(WordCountOptions.class);
		    Pipeline p = Pipeline.create(options);

		    PCollection<KV<String, Long>> counts = p.apply("ReadLines Files", TextIO.read().from(options.getInputFile()))
		    										.apply(new CountWords());
		     
		     counts.apply(MapElements.via(new FormatAsTextFn()))
		     	   .apply("WriteCounts", TextIO.write().to(options.getOutput()));

		     counts.apply(MapElements.via(new FormatAsTableRow()))
			   	   .apply("WriteCounts in BigQuery", 
			   			 BigQueryIO.writeTableRows()
				   	        .to(options.getTableCompleteBQ())
				   	        .withSchema(WordCount.buildSchema())
				   	        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
				   	        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
		    p.run().waitUntilFinish();
		  }
}
