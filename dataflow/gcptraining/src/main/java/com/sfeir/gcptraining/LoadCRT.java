package com.sfeir.gcptraining;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.ValueInSingleWindow;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

public class LoadCRT {

	public interface CRTOptions extends PipelineOptions, BigQueryOptions {
			
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
	    @Default.String("*")
	    String getInputFile();
	    void setInputFile(String value);
	
	    /**
	     * Set this required option to specify where to write the output.
	     */
	    @Description("Table for BigQuery : dataset.table")
	    @Required
	    String getDataSetBQ();
	    void setDataSetBQ(String value);  

	    /**
	     * Set this required option to specify where to write the output.
	     */
	    @Description("dataset.table")
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
				CRTOptions woptions = options.as(CRTOptions.class);
				return woptions.getProject() + ":" + woptions.getDataSetBQ() + woptions.getTableBQ();
			}
	    }
	
	}
	
	static class PopulateTableRow extends DoFn<String, TableRow> {

		  @ProcessElement
		  public void processElement(ProcessContext c) {
			  String elmt = c.element();
			  String[] elmtTab = elmt.split(";");
			  TableRow row = new TableRow();
			  row.set("sensorId", elmtTab[0]);
			  row.set("siteId", elmtTab[1]);
			  row.set("event_time", elmtTab[2]);
			  row.set("value", elmtTab[3]);
			  c.output(row);
		  }
	}
	
	/**
	 * Helper method to build the table schema for the output table.
	 */
	public static TableSchema buildSchema() {
	    List<TableFieldSchema> fields = new ArrayList<>();
	    fields.add(new TableFieldSchema().setName("sensorId").setType("STRING"));
	    fields.add(new TableFieldSchema().setName("siteId").setType("STRING"));
	    fields.add(new TableFieldSchema().setName("event_time").setType("STRING"));
	    fields.add(new TableFieldSchema().setName("value").setType("STRING"));
	    TableSchema schema = new TableSchema().setFields(fields);
	    return schema;
	}
	  
	public static void main(String[] args) {
	    CRTOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(CRTOptions.class);
		Pipeline p = Pipeline.create(options);
	
		p.apply("ReadLines Files", TextIO.read().from(options.getBucketFile() + options.getInputFile()))
		 .apply("populate row",ParDo.of(new PopulateTableRow()))
//		 .apply("apply timestamp", WithTimestamps.<TableRow> of(t -> Instant.parse(((String) t.get("event_time")))))
		 .apply("WriteCounts in BigQuery", 
		BigQueryIO.writeTableRows()
				 .to(new SerializableFunction<ValueInSingleWindow<TableRow>, TableDestination>() {
			  @Override
			  public TableDestination apply(ValueInSingleWindow<TableRow> value) {  
				String partition = ((String) value.getValue().get("event_time")).substring(0, 10).replace("/", "");
			    return new TableDestination("sfeir-bucket:data.filetest"+"$"+partition, null);
			  }
			})
		
				  .withSchema(buildSchema())
			   	  .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
			   	  .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
	    p.run().waitUntilFinish();
	}
	  
	  
}
