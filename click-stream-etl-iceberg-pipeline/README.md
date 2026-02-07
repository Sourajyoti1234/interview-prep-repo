# Real-Time Clickstream Lakehouse on AWS

(Kinesis · AWS Glue · Amazon S3 · Apache Iceberg)

An end-to-end **real-time data engineering project** that demonstrates how to build a modern **AWS Lakehouse architecture** using **Amazon Kinesis Data Streams, AWS Glue (Spark Structured Streaming), Amazon S3, and Apache Iceberg**.

This project simulates **real-world clickstream data** using **AWS Lambda**, processes it in near real time, and stores it in **ACID-compliant Iceberg tables** that support schema evolution and time travel.

---

## 🚀 Architecture Overview

```
AWS Lambda (Clickstream Producer)
        |
        v
Amazon Kinesis Data Streams
        |
        v
AWS Glue Streaming Job (Spark)
        |
        v
Amazon S3 (Apache Iceberg Tables)
        |
        v
Amazon Athena / BI Tools
```

---

## 🎯 Project Goals

- Generate realistic **clickstream events** using AWS Lambda
- Stream events in real time using **Amazon Kinesis**
- Process data using **AWS Glue Structured Streaming**
- Store data in **Apache Iceberg tables on Amazon S3**
- Enable **ACID transactions, schema evolution, and time travel**
- Make data query-ready using **Amazon Athena**

---

## 🧱 Tech Stack

| Layer        | Technology                            |
| ------------ | ------------------------------------- |
| Producer     | AWS Lambda (Python)                   |
| Streaming    | Amazon Kinesis Data Streams           |
| Processing   | AWS Glue (Spark Structured Streaming) |
| Storage      | Amazon S3                             |
| Table Format | Apache Iceberg                        |
| Metadata     | AWS Glue Data Catalog                 |
| Query        | Amazon Athena                         |

---

## 📂 Data Scenario: Clickstream Events

Each event represents a user interaction on a website.

### Sample Event Schema

```json
{
  "run_id": "uuid",
  "event_id": "uuid",
  "user_id": 10045,
  "session_id": "uuid",
  "country": "IN",
  "device": "mobile",
  "event_type": "click",
  "page": "/product",
  "click_time": "2026-02-07T10:15:30Z",
  "leave_time": "2026-02-07T10:17:10Z",
  "session_duration_sec": 100
}
```

---

## 🧪 AWS Lambda Producer

- Generates **synthetic yet realistic** clickstream data
- Sends records to Kinesis using **batch writes (`put_records`)**
- Configurable number of events per run
- Uses `user_id` / `session_id` as **PartitionKey** for shard-level parallelism
- Supports event-time simulation and late-arriving data

**Directory:**

```
lambda-producer/
 └── lambda_function.py
```

---

## 🔄 AWS Glue Streaming Job

- Reads streaming data from Kinesis
- Parses JSON payloads
- Applies schema enforcement and transformations
- Handles late-arriving events using event time
- Writes processed data into **Apache Iceberg tables on S3**

**Directory:**

```
glue-streaming-job/
 └── glue_kinesis_to_iceberg.py
```

---

## 🧊 Apache Iceberg Features Used

- ✅ ACID-compliant streaming writes
- ✅ Schema evolution without table rewrites
- ✅ Time travel for data debugging and auditing
- ✅ Optimized partition handling
- ✅ Unified batch and streaming reads

---

## 📊 Sample Analytics (Amazon Athena)

```sql
SELECT
  country,
  device,
  COUNT(*) AS total_clicks
FROM clickstream_events
WHERE click_time >= current_timestamp - interval '1' hour
GROUP BY country, device;
```

---

## 🔐 IAM & Security

- **Lambda IAM Role**
  - `kinesis:PutRecord`
  - `kinesis:PutRecords`

- **Glue Job IAM Role**
  - Read from Kinesis
  - Write to Amazon S3
  - Access Glue Data Catalog

Principle followed: **Least Privilege Access**

---

## 📈 Future Enhancements

- Late data watermarking
- CDC-style UPSERTs using Iceberg `MERGE INTO`
- Data quality checks
- CloudWatch monitoring and alerting
- Redshift Spectrum integration
- BI dashboards (QuickSight / Power BI)

---

## 🧠 Interview-Ready Summary

> Built a real-time AWS Lakehouse where clickstream data is generated using AWS Lambda, streamed through Amazon Kinesis, processed via AWS Glue Structured Streaming, and stored as Apache Iceberg tables on Amazon S3, enabling ACID transactions, schema evolution, and time travel.

---

## 📁 Repository Structure

```
.
├── lambda-producer/
│   └── lambda_function.py
├── glue-streaming-job/
│   └── glue_kinesis_to_iceberg.py
├── iceberg-ddl/
│   └── create_tables.sql
├── architecture/
│   └── architecture-diagram.png
└── README.md
```

---

## 🤝 Contributions

Suggestions, improvements, and contributions are welcome.

---

## 📜 License

This project is intended for learning and demonstration purposes.
