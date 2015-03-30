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

public class Driver {
	private float distancePartition;
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

	public float getDistancePartition() {
		return distancePartition;
	}

	public void setDistancePartition(float distancePartition) {
		this.distancePartition = distancePartition;
	}

	public static void main(String[] args) throws Exception {
		Driver driverObj = new Driver();
		driverObj.setInputFilePath(args[0]);
		driverObj.setOutputFilePath(args[1]);
		driverObj.setDistancePartition(Float.parseFloat(args[2]));

		driverObj.runJobs();

	}

	void runJobs() throws IOException {
		ReceiptJob receiptJobObj = new ReceiptJob();
		receiptJobObj.setInputFilePath(inputFilePath);
		receiptJobObj.setOutputFilePath(outputFilePath + "/customer");
		receiptJobObj.run();

		CustomerJob customerJobObj = new CustomerJob();
		customerJobObj.setInputFilePath(outputFilePath + "/customer");
		customerJobObj.setOutputFilePath(outputFilePath + "/store");
		customerJobObj.setDistancePartition(distancePartition);
		customerJobObj.run();

		StoreJob storeJobObj = new StoreJob();
		storeJobObj.setInputFilePath(outputFilePath + "/store");
		storeJobObj.setOutputFilePath(outputFilePath + "/out");
		storeJobObj.run();

	}
}
