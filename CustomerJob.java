package hackathon;

/**
 * 
 * @author pavan
 * @author pramodh
 * 
 *  Map -reducer class to combine the entries to receipt level
 *
 */

import java.io.IOException;
import java.text.DecimalFormat;
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

public class CustomerJob {
	private String inputFilePath, outputFilePath;
	private float distancePartition;

	public float getDistancePartition() {
		return distancePartition;
	}

	public void setDistancePartition(float distancePartition) {
		this.distancePartition = distancePartition;
	}

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

	public static class CustomerMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		// Input : key - offset, value - customer|homeStore
		// tier|distance|isHomeStore|itemCountPerReceipt|totalValueOfReceipt
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String[] tokens = value.toString().split("\\t");

			// Output : key - customer|homeStore, value -
			// tier|distance|isHomeStore|itemCountPerReceipt|totalValueOfReceipt
			output.collect(new Text(tokens[0]), new Text(tokens[1]));

		}
	}

	public static class CustomerReducer extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		// Input : key - customer|homeStore, value -
		// tier|distance|isHomeStore|itemCountPerReceipt|totalValueOfReceipt
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String keyTokens[] = key.toString().split("\\|");
			String tokens[] = null;
			float homeSales = 0.0f, nonHomeSales = 0.0f, distance;
			int homeRecieptCount = 0, nonHomeReceiptCount = 0;
			String homeStore = "", tier = "";
			DecimalFormat df = new DecimalFormat("0.##");

			while (values.hasNext()) {
				tokens = values.next().toString().split("\\|");
				if (Boolean.parseBoolean(tokens[2])) {
					homeRecieptCount++;
					homeSales += Float.parseFloat(tokens[4]);
				} else {
					nonHomeReceiptCount++;
					nonHomeSales += Float.parseFloat(tokens[4]);
				}

			}

			homeStore = keyTokens[1];
			tier = tokens[0];
			distance = categorizeDistance(tokens[1]);

			// Output: key - HomeStore|Distance, value -
			// Tier|HomeReceiptCount|HomeStoreTotalSalesAmount|Non-homeStoreTotalSalesAmount
			output.collect(new Text(homeStore + "|" + df.format(distance)),
					new Text(tier + "|" + homeRecieptCount + "|" + homeSales
							+ "|" + nonHomeReceiptCount + "|" + nonHomeSales));

		}

		float categorizeDistance(String distance) {
			float distanceRange = 0.5f;
			int partitions = (int) (Float.parseFloat(distance) / distanceRange);
			return ((partitions + 1) * distanceRange);

		}
	}

	void run() throws IOException {
		JobConf conf = new JobConf(CustomerJob.class);
		conf.setJobName("customerdata");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(CustomerMapper.class);
		conf.setReducerClass(CustomerReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(inputFilePath));
		FileOutputFormat.setOutputPath(conf, new Path(outputFilePath));

		JobClient.runJob(conf);
	}

}
