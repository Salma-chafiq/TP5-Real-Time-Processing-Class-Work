# Kafka Stream Processing: Total Amount per Customer

## Description

This project demonstrates the use of Kafka Streams to calculate the total order amount per customer. It processes data from the `orders` topic and outputs the aggregated totals to the `customer-total` topic. The results can also be stored in HDFS for further analysis.

---

## Prerequisites

1. **Java 8 or later** (Ensure it's installed and properly configured.)
2. **Apache Kafka** (Installed locally or running in Docker.)
3. **Hadoop (HDFS)** for storing aggregated data.
4. **Maven** (Dependency management and project building.)
5. **Docker/Docker Compose** (Optional for running Kafka and Zookeeper.)

---

## Project Setup

### Step 1: Setting Up Kafka Topics

Start Kafka and Zookeeper, then create the required topics:

```bash
# Create the 'orders' topic
kafka-topics --create --topic orders --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# Create the 'customer-total' topic
kafka-topics --create --topic customer-total --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### Step 2: Producing Sample Data to the `orders` Topic

Once the topics are set up, produce some sample data to the `orders` topic to simulate order transactions.

1. Use the Kafka console producer to send data:

   ```bash
   kafka-console-producer --topic orders --bootstrap-server localhost:9092
   ```

2. Enter sample messages in the format `<customer_id>,<order_amount>`:

   ```plaintext
   customer1,100.5
   customer2,200.75
   customer1,50.25
   customer3,300.00
   ```

   Each message represents an order with a `customer_id` and an `order_amount`.

---

### Step 3: Running the Kafka Streams Application

Run the Kafka Streams application that processes the `orders` topic to compute the total amount per customer and outputs it to the `customer-total` topic.

1. Build the project using Maven:

   ```bash
   mvn clean install
   ```

2. Execute the Kafka Streams application (e.g., `App2.java`):

   ```bash
   mvn exec:java -Dexec.mainClass="org.example.App2"
   ```

   This will start the application and continuously process the data from the `orders` topic.

---

### Step 4: Verifying the Output in the `customer-total` Topic

After running the Kafka Streams application, verify that the processed data has been written to the `customer-total` topic.

1. Use the Kafka console consumer to read messages from the `customer-total` topic:

   ```bash
   kafka-console-consumer --topic customer-total --from-beginning --bootstrap-server localhost:9092
   ```

2. The output should display the total order amount per customer in real time:

   ```plaintext
   customer1,150.75
   customer2,200.75
   customer3,300.00
   ```

---

## Optional: Writing Processed Data to HDFS

### Step 5: Setting Up HDFS Sink Connector

To store the processed data in HDFS, configure the Kafka HDFS Sink Connector.

1. Create a configuration file for the HDFS Sink Connector (e.g., `hdfs-sink.properties`):

   ```properties
   name=hdfs-sink
   connector.class=io.confluent.connect.hdfs.HdfsSinkConnector
   tasks.max=1
   topics=customer-total
   hdfs.url=hdfs://localhost:9000
   flush.size=3
   ```

2. Start the Kafka Connect process with the HDFS Sink Connector configuration:

   ```bash
   connect-standalone connect-standalone.properties hdfs-sink.properties
   ```

---

### Step 6: Verifying Data in HDFS

Once the Kafka Connect process is running, check if the data has been successfully stored in HDFS.

1. Use the following command to list the files in the HDFS directory:

   ```bash
   hdfs dfs -ls /kafka-data/customer-total
   ```

2. View the content of a file to confirm the stored data:

   ```bash
   hdfs dfs -cat /kafka-data/customer-total/<file-name>
   ```

   The file should contain the aggregated totals for each customer.

---

## Adding Images to the README

Below are visual representations of the setup process and results:

### Kafka Stream Workflow

![Kafka Stream Workflow](Capture/capture1.png)

### HDFS Data Flow

![HDFS Data Flow](Capture/capture2.png)

---

## Troubleshooting

- **Kafka topic creation issues:** Ensure Kafka and Zookeeper are running correctly before creating topics.
- **HDFS connectivity issues:** Verify that the HDFS cluster is running and accessible from the Kafka Connect process.
- **Kafka Streams application errors:** Check the application logs for any runtime exceptions or misconfigurations.

---

## Additional Notes

- This project can be extended to include more complex aggregation logic.
- The data in HDFS can be analyzed using tools like Apache Hive or Apache Spark.
- Ensure proper security configurations when deploying this project in a production environment.

---
