# mapreduce-fraudcount
Example created by me while following udemy course

**We will use a ecommerce order dataset to analyse the fraud possibility tax according to the following business rules**

The business rule is to assign a fraud point for each line where the difference between the order receive date and the devolution date  is bigger than ten days

We should also assign ten fraud points for each customer (Counting all his orders) whose devolution rate is bigger than half of his total orders

The dataset used is at resources\dataset folder

**To execute you will need a local hadoop instance and use the following commands after exporting the project classes to a .jar**
```
hadoop jar /mnt/c/Users/renan.rodrigues/Downloads/mapreduce-fraudcount.jar file:////home/rodrir23/fraud.txt/ file:////home/rodrir23/output-fraud/
```

The first argument is the dataset which should be placed in the directory corresponding to what we are going to pass to the main class
The second can be any directory in which the job result will be stored

**To view the results**
```
hadoop fs -cat /home/rodrir23/output-fraud/part-r-00000
```
