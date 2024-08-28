# ETL-Pipeline-for-Real-Time-News-Data-Kafka-HDFS-Hive-Implementation

News Data ETL Pipeline

Business Scenario:  
The project involves setting up an ETL pipeline to collect real-time news data from various sources, process it, and store it for further analysis. The pipeline uses Apache Kafka for data ingestion, HDFS for storage, and Apache Hive for querying and analyzing the data. The objective is to streamline the process of gathering news articles on specific topics and gain valuable insights from the data using Hive.

Tasks and Questions-:

1. Kafka Broker Setup:  
   - Set up a Kafka broker with Zookeeper to handle the data stream. The broker will manage the data flow between producers (data ingestion from NewsAPI) and consumers (storing data in HDFS).
   
2. NewsAPI Integration:
   - Obtain a free API token from NewsAPI.
   - Write Python code to retrieve news articles based on specific keywords related to a chosen topic.
   - Experiment with the data returned from NewsAPI to understand its structure and contents.

3. Kafka Producer Setup:
   - Develop a Kafka producer in Python to fetch news articles from NewsAPI and send the data to a Kafka topic. Ensure the data is well-structured for downstream processing.

4. Kafka Consumer and Data Storage:
   - Set up a Kafka consumer to continuously read the news articles from the Kafka topic and store them in HDFS. The data can be saved directly using Python or transferred from a Linux file system to HDFS.

5. Data Analysis with Apache Hive:
   - Create a Hive table to store and analyze the news data.
   - Write Hive queries to gain insights from the data, such as:
     - Identify the most common topics in news articles over the past month.
     - Find the top 5 sources of news articles for the chosen keywords.
     - Calculate the average number of articles published per day.
     - Determine the sentiment distribution (positive/negative/neutral) across the articles.
- Analyze the geographical distribution of news sources.



