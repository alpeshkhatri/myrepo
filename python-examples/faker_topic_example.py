from faker import Faker
import random
from datetime import datetime, timedelta

# Initialize Faker
fake = Faker()

def generate_topic_name():
    """Generate realistic Kafka topic names"""
    topic_types = [
        f"user-{fake.word()}",
        f"order-{fake.word()}",
        f"payment-{fake.word()}",
        f"inventory-{fake.word()}",
        f"notification-{fake.word()}",
        f"{fake.company().lower().replace(' ', '-')}-events",
        f"{fake.word()}-stream",
        f"{fake.word()}-logs",
        f"analytics-{fake.word()}",
        f"metrics-{fake.word()}"
    ]
    return random.choice(topic_types).replace("'", "").replace(",", "")

def generate_offset_details(topic_name, num_partitions=None):
    """Generate offset details for a topic"""
    if num_partitions is None:
        num_partitions = random.randint(1, 12)
    
    partitions = []
    for partition_id in range(num_partitions):
        # Generate realistic offset values
        current_offset = random.randint(1000, 100000)
        lag = random.randint(0, 500)
        earliest_offset = random.randint(0, current_offset - 1000)
        
        partition_info = {
            "partition": partition_id,
            "current_offset": current_offset,
            "earliest_offset": earliest_offset,
            "latest_offset": current_offset + lag,
            "lag": lag,
            "consumer_group": fake.slug(),
            "last_updated": fake.date_time_between(start_date='-1d', end_date='now')
        }
        partitions.append(partition_info)
    
    return {
        "topic_name": topic_name,
        "total_partitions": num_partitions,
        "partitions": partitions,
        "created_at": fake.date_time_between(start_date='-30d', end_date='-1d')
    }

def generate_consumer_group_info():
    """Generate consumer group information"""
    return {
        "group_id": fake.slug(),
        "state": random.choice(["Stable", "PreparingRebalance", "CompletingRebalance", "Dead"]),
        "protocol": "range",
        "members": [
            {
                "member_id": fake.uuid4(),
                "client_id": f"{fake.word()}-consumer-{random.randint(1, 10)}",
                "host": fake.ipv4(),
                "assigned_partitions": random.randint(1, 5)
            } for _ in range(random.randint(1, 5))
        ]
    }

# Example usage
if __name__ == "__main__":
    print("=== Kafka Topic and Offset Generator ===\n")
    
    # Generate multiple topics with offset details
    for i in range(5):
        topic = generate_topic_name()
        offset_info = generate_offset_details(topic)
        
        print(f"Topic: {offset_info['topic_name']}")
        print(f"Created: {offset_info['created_at']}")
        print(f"Partitions: {offset_info['total_partitions']}")
        print("Partition Details:")
        
        for partition in offset_info['partitions']:
            print(f"  Partition {partition['partition']}:")
            print(f"    Current Offset: {partition['current_offset']:,}")
            print(f"    Latest Offset:  {partition['latest_offset']:,}")
            print(f"    Earliest Offset: {partition['earliest_offset']:,}")
            print(f"    Lag: {partition['lag']:,}")
            print(f"    Consumer Group: {partition['consumer_group']}")
            print(f"    Last Updated: {partition['last_updated']}")
        
        print("-" * 50)
    
    # Generate consumer group information
    print("\n=== Consumer Group Information ===")
    consumer_group = generate_consumer_group_info()
    print(f"Group ID: {consumer_group['group_id']}")
    print(f"State: {consumer_group['state']}")
    print(f"Protocol: {consumer_group['protocol']}")
    print("Members:")
    for member in consumer_group['members']:
        print(f"  - Member ID: {member['member_id']}")
        print(f"    Client ID: {member['client_id']}")
        print(f"    Host: {member['host']}")
        print(f"    Assigned Partitions: {member['assigned_partitions']}")
    
    # Generate batch of topic names only
    print("\n=== Topic Names Only ===")
    topic_names = [generate_topic_name() for _ in range(10)]
    for name in topic_names:
        print(f"- {name}")

# Additional utility functions
def generate_topic_metrics():
    """Generate topic performance metrics"""
    return {
        "messages_per_second": round(random.uniform(10.5, 1000.5), 2),
        "bytes_per_second": random.randint(1024, 1048576),  # 1KB to 1MB
        "avg_message_size": random.randint(100, 5000),
        "peak_throughput": round(random.uniform(100.0, 5000.0), 2),
        "error_rate": round(random.uniform(0.0, 2.5), 3),
        "retention_hours": random.choice([24, 48, 168, 720]),  # 1 day to 30 days
        "compression_type": random.choice(["none", "gzip", "snappy", "lz4", "zstd"])
    }

def generate_kafka_cluster_info():
    """Generate Kafka cluster information"""
    num_brokers = random.randint(3, 9)
    brokers = []
    
    for i in range(num_brokers):
        brokers.append({
            "broker_id": i,
            "host": fake.ipv4(),
            "port": 9092,
            "is_controller": i == 0,  # First broker is controller
            "disk_usage_mb": random.randint(10000, 100000),
            "network_throughput_mbps": round(random.uniform(10.0, 1000.0), 2)
        })
    
    return {
        "cluster_id": fake.uuid4(),
        "brokers": brokers,
        "total_topics": random.randint(50, 500),
        "total_partitions": random.randint(200, 2000),
        "version": random.choice(["2.8.0", "3.0.0", "3.1.0", "3.2.0", "3.3.0"])
    }
