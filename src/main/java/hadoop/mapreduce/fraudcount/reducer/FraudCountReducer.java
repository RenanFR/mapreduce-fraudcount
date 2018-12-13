package hadoop.mapreduce.fraudcount.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import hadoop.mapreduce.fraudcount.pojo.CustomerFraud;

public class FraudCountReducer extends Reducer<Text, CustomerFraud, Text, IntWritable>{

	@Override
	protected void reduce(Text mapOutputKey, Iterable<CustomerFraud> mapOutputValue,
			Reducer<Text, CustomerFraud, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		int sumCustomerFraudPoints = 0;
		int ordersCount = 0;
		int devolutionCount = 0;
		for (CustomerFraud cf : mapOutputValue) {
			ordersCount++;
			sumCustomerFraudPoints += cf.getFraudPoints();
			if (cf.isOrderReturned()) {
				devolutionCount++;
			}
		}
		double devolutionRate = (devolutionCount / (ordersCount * 1.0)) * 100;
		if (devolutionRate >= 50) {
			sumCustomerFraudPoints += 10;
		}
		context.write(mapOutputKey, new IntWritable(sumCustomerFraudPoints));
	}
	
}
