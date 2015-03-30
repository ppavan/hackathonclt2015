package hackathon;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 * 
 * @author pavan
 * @author pramodh
 * 
 *  Map -reducer class to combine the entries to receipt level
 *
 */

public class ReceiptJob {
	private String inputFilePath, outputFilePath;

	public String getInputFilePath() {
		return inputFilePath;
	}

	public void setInputFilePath(String inputFilePath) {
		this.inputFilePath = inputFilePath;
	}

	public String getOutputFilePath() {
		return outputFilePath;
	}

	public void setOutputFilePath(String outputFilePath) {
		this.outputFilePath = outputFilePath;
	}

	public static class ReceiptMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		// Input: key - offset, value - raw data entry
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			// This MAP function would parse the input file and extract required
			// attributes.

			String[] tokens = value.toString().split("\\|");
			String customer = tokens[0];
			String tier = tokens[1];
			String homeStore = tokens[2];
			String distance = tokens[5];
			String receipt = tokens[6];
			String store = tokens[9];
			String sales = tokens[21];

			// Output: key - receipt, value -
			// customer|homeStore|tier|distance|store|sales
			output.collect(new Text(receipt), new Text(customer + "|"
					+ homeStore + "|" + tier + "|" + distance + "|" + store
					+ "|" + sales));

		}
	}

	public static class ReceiptReducer extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		// Input: key - receipt, values -
		// customer|homeStore|tier|distance|store|sales
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String tokens[] = null;
			float sales = 0.0f;
			int itemCount = 0;
			String customer = "", homeStore = "", tier = "", distance = "";
			boolean isHome;

			while (values.hasNext()) {
				itemCount++;
				tokens = values.next().toString().split("\\|");
				sales += Float.parseFloat(tokens[5]);
			}

			customer = tokens[0];
			homeStore = tokens[1];
			distance = tokens[3];
			isHome = homeStore.equals(tokens[4]) ? true : false;
			tier = tokens[2];

			// Output : key - customer|homeStore, value -
			// tier|distance|isHomeStore|itemCountPerReceipt|totalValueOfReceipt
			output.collect(new Text(customer + "|" + homeStore), new Text(tier
					+ "|" + distance + "|" + isHome + "|" + itemCount + "|"
					+ sales));

		}
	}

	void run() throws IOException {
		JobConf conf = new JobConf(ReceiptJob.class);
		conf.setJobName("extractattr");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(ReceiptMapper.class);
		// conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(ReceiptReducer.class);
		// conf.setReducerClass(CustomerStoreReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(inputFilePath));
		FileOutputFormat.setOutputPath(conf, new Path(outputFilePath));

		JobClient.runJob(conf);
	}

}
