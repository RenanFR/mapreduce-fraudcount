package hadoop.mapreduce.fraudcount.pojo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CustomerFraud implements WritableComparable<CustomerFraud>{
	
	private String customerName;
	
	private String customerId;
	
	private String receivedDate;
	
	private boolean orderReturned;
	
	private String returnDate;

	private int fraudPoints;
	
	public CustomerFraud() {
		this.customerId = "";
		this.customerName = "";
		this.orderReturned = false;
		this.receivedDate = "";
		this.returnDate = "";
		this.fraudPoints = 0;
	}

	public String getCustomerName() {
		return customerName;
	}

	public void setCustomerName(String customerName) {
		this.customerName = customerName;
	}

	public String getCustomerId() {
		return customerId;
	}

	public void setCustomerId(String customerId) {
		this.customerId = customerId;
	}

	public String getReceivedDate() {
		return receivedDate;
	}

	public void setReceivedDate(String receivedDate) {
		this.receivedDate = receivedDate;
	}

	public boolean isOrderReturned() {
		return orderReturned;
	}

	public void setOrderReturned(boolean orderReturned) {
		this.orderReturned = orderReturned;
	}

	public String getReturnDate() {
		return returnDate;
	}

	public void setReturnDate(String returnDate) {
		this.returnDate = returnDate;
	}

	public int getFraudPoints() {
		return fraudPoints;
	}

	public void setFraudPoints(int fraudPoints) {
		this.fraudPoints = fraudPoints;
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.customerId = WritableUtils.readString(dataInput);
		this.customerName = WritableUtils.readString(dataInput);
		this.orderReturned = dataInput.readBoolean();
		this.receivedDate = WritableUtils.readString(dataInput);
		this.fraudPoints = dataInput.readInt();
		this.returnDate = WritableUtils.readString(dataInput);
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeString(dataOutput, this.customerId);
		WritableUtils.writeString(dataOutput, this.customerName);
		dataOutput.writeBoolean(this.orderReturned);
		WritableUtils.writeString(dataOutput, this.receivedDate);
		dataOutput.writeInt(this.fraudPoints);
		WritableUtils.writeString(dataOutput, this.returnDate);
	}

	@Override
	public int compareTo(CustomerFraud o) {
		return this.getCustomerName().compareTo(o.getCustomerName());
	}

	@Override
	public String toString() {
		return "CustomerFraud [customerName=" + customerName + ", customerId=" + customerId + ", receivedDate="
				+ receivedDate + ", orderReturned=" + orderReturned + ", returnDate=" + returnDate + ", fraudPoints="
				+ fraudPoints + "]";
	}

}
