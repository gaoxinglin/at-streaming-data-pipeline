"""
Spike: Redpanda → Spark Structured Streaming → print
验证目标: Spark 能从 Redpanda 读到消息并打印
"""
import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
TOPIC = "at.trip_updates"

# kafka-clients jar — Spark 连 Kafka 需要这个
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"


def main():
    spark = (
        SparkSession.builder.appName("spike_consumer")
        .config("spark.jars.packages", KAFKA_PACKAGE)
        # 关掉不必要的日志，spike 只看数据
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"[Spark] 开始监听 topic: {TOPIC}")
    print("[Spark] 等待消息... (先运行 spike_producer.py 推几条消息)\n")

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    # 只看 value，转成字符串
    messages = df.selectExpr("CAST(value AS STRING) as message")

    query = (
        messages.writeStream.format("console")
        .option("truncate", False)
        .outputMode("append")
        .start()
    )

    query.awaitTermination(timeout=30)  # 30秒后自动停止
    print("\n✅ Spike 成功: Spark 能从 Redpanda 读取消息")


if __name__ == "__main__":
    main()
