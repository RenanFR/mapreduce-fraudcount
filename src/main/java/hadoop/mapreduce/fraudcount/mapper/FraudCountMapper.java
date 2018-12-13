package hadoop.mapreduce.fraudcount.mapper;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import hadoop.mapreduce.fraudcount.pojo.CustomerFraud;

//GGYZ333519YS,Allison,08-01-2017,10-01-2017,Delhivery,13-01-2017,yes,15-01-2017,Damaged Item

public class FraudCountMapper extends Mapper<LongWritable, Text, Text, CustomerFraud>{
	
	private CustomerFraud customerFraud = new CustomerFraud();
	
	@Override
	protected void map(LongWritable index, Text textLine,
			Mapper<LongWritable, Text, Text, CustomerFraud>.Context context)
			throws IOException, InterruptedException {
		int fraudPoints = 0;
		String[] datasetLine = textLine.toString().split(",");
		String customerName = datasetLine[1];
		String customerId = datasetLine[0];
		boolean orderReturned = datasetLine[6].equalsIgnoreCase("yes");
		String receivedDateStr = datasetLine[5];
		customerFraud.setCustomerId(customerId);
		customerFraud.setCustomerName(customerName);
		customerFraud.setReceivedDate(receivedDateStr);
		customerFraud.setOrderReturned(orderReturned);
		String returnDateStr = datasetLine[7];
		if (orderReturned) {
			customerFraud.setReturnDate(returnDateStr);
		} else {
			customerFraud.setReturnDate("null");//if the attribute is not set the it will use the value of the last register
		}
		if (customerFraud.isOrderReturned()) {
			LocalDate returnDate = LocalDate.parse(returnDateStr, DateTimeFormatter.ofPattern("dd-MM-yyyy"));
			LocalDate receivedDate = LocalDate.parse(receivedDateStr, DateTimeFormatter.ofPattern("dd-MM-yyyy"));
			if (ChronoUnit.DAYS.between(receivedDate, returnDate) > 10) {
				fraudPoints = 1;
			}
		}
		customerFraud.setFraudPoints(fraudPoints);
		context.write(new Text(customerFraud.getCustomerName()), customerFraud);
	}
	
}
