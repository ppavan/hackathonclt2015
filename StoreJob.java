package hackathon;

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

/**
 * 
 * @author pavan
 * @author pramodh
 * 
 *  Map -reducer class to combine the entries to receipt level
 *
 */

public class StoreJob {
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

	public static class StoreMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		
		// Output: key - HomeStore|Distance, value -
		// Tier|HomeReceiptCount|HomeStoreTotalSalesAmount|Non-homeStoreTotalSalesAmount

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {


			String[] tokens = value.toString().split("\\t");

			output.collect(new Text(tokens[0].replace('|', ',') + ","),
					new Text(tokens[1]));

		}
	}

	public static class StoreReducer extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			// String keyTokens[] = key.toString().split("\\|");
			String tokens[] = null;
			int customerCount = 0;
			int homeReceiptCount, nonHomeReceiptCount;
			float homeSales, nonHomeSales;
			float avgHomeVisitPercent = 0.0f;
			float avgHomeSalePercent = 0.0f;
			int[] tier = new int[5];
			float homeVisitPercent = 0.0f;
			float homeSalePercent = 0.0f;

			DecimalFormat df = new DecimalFormat("0.##");

			while (values.hasNext()) {
				tokens = values.next().toString().split("\\|");
				customerCount++;
				try {
					tier[Integer.parseInt(tokens[0])]++;
				} catch (NumberFormatException nexception) {
					tier[0]++;
				}
				homeReceiptCount = Integer.parseInt(tokens[1]);
				homeSales = Float.parseFloat(tokens[2]);
				nonHomeReceiptCount = Integer.parseInt(tokens[3]);
				nonHomeSales = Float.parseFloat(tokens[4]);
				homeVisitPercent += ((float) homeReceiptCount / (homeReceiptCount + nonHomeReceiptCount)) * 100;
				homeSalePercent += (homeSales / (homeSales + nonHomeSales)) * 100;

			}
			avgHomeVisitPercent = ((float) homeVisitPercent / customerCount);
			avgHomeSalePercent = homeSalePercent / customerCount;

			output.collect(
					key,
					new Text("," + customerCount + ","
							+ df.format(avgHomeVisitPercent) + ","
							+ df.format(avgHomeSalePercent) + "," + tier[0]
							+ "," + tier[1] + "," + tier[2] + "," + tier[3]
							+ "," + tier[4]));

		}

	}

	void run() throws IOException {
		JobConf conf = new JobConf(StoreJob.class);
		conf.setJobName("storedata");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(StoreMapper.class);
		conf.setReducerClass(StoreReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(inputFilePath));
		FileOutputFormat.setOutputPath(conf, new Path(outputFilePath));

		JobClient.runJob(conf);
	}

}
