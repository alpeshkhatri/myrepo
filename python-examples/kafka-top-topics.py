#!/usr/bin/env python3
"""
Kafka Top Topics Monitor - Shows topics with highest message production
"""
import argparse
import time
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Tuple

from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, ConfigResource
from tabulate import tabulate


class KafkaTopTopicsMonitor:
    def __init__(
        self,
        bootstrap_servers: str,
        num_topics: int = 10,
        refresh_interval: int = 10,
    ):
        """
        Initialize the Kafka top topics monitor.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers (comma-separated list)
            num_topics: Number of top topics to display
            refresh_interval: Refresh interval in seconds
        """
        self.bootstrap_servers = bootstrap_servers
        self.num_topics = num_topics
        self.refresh_interval = refresh_interval
        
        # Configure the admin client
        self.admin_client = AdminClient({
            'bootstrap.servers': bootstrap_servers
        })
        
        # Configure the consumer for metadata access
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': f'kafka-top-topics-{int(time.time())}',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': False
        })
        
        self.previous_offsets: Dict[str, Dict[int, int]] = defaultdict(dict)
        self.current_offsets: Dict[str, Dict[int, int]] = defaultdict(dict)
        self.message_rates: Dict[str, int] = defaultdict(int)

    def get_all_topics(self) -> List[str]:
        """Get all topics in the Kafka cluster."""
        metadata = self.admin_client.list_topics(timeout=10)
        return list(metadata.topics.keys())

    def get_topic_offsets(self, topic: str) -> Dict[int, int]:
        """Get the latest offsets for all partitions in a topic."""
        partitions = {}
        
        # Get topic metadata to determine number of partitions
        metadata = self.consumer.list_topics(topic, timeout=10)
        if topic not in metadata.topics:
            return partitions
            
        topic_partitions = metadata.topics[topic].partitions
        
        # Get the end offset for each partition
        for partition_id in topic_partitions:
            # Get watermark offsets (low, high)
            low, high = self.consumer.get_watermark_offsets(
                f"{topic}:{partition_id}", timeout=10
            )
            partitions[partition_id] = high
            
        return partitions

    def collect_offsets(self):
        """Collect current offsets for all topics."""
        # Move current offsets to previous
        self.previous_offsets = self.current_offsets
        self.current_offsets = defaultdict(dict)
        
        # Get all topics
        topics = self.get_all_topics()
        
        # Skip internal topics
        topics = [t for t in topics if not t.startswith('_')]
        
        # Collect current offsets
        for topic in topics:
            try:
                self.current_offsets[topic] = self.get_topic_offsets(topic)
            except Exception as e:
                print(f"Error getting offsets for topic {topic}: {e}")

    def calculate_message_rates(self):
        """Calculate message production rates for all topics."""
        self.message_rates = defaultdict(int)
        
        for topic in self.current_offsets:
            topic_message_count = 0
            
            # For each partition, calculate the difference in offsets
            for partition, current_offset in self.current_offsets[topic].items():
                previous_offset = self.previous_offsets.get(topic, {}).get(partition, current_offset)
                partition_message_count = max(0, current_offset - previous_offset)
                topic_message_count += partition_message_count
                
            # Store the message rate (messages per second)
            self.message_rates[topic] = topic_message_count / self.refresh_interval

    def get_top_topics(self) -> List[Tuple[str, float, int]]:
        """Get the top topics by message rate."""
        # Calculate total partition count for each topic
        topic_partition_counts = {
            topic: len(partitions) 
            for topic, partitions in self.current_offsets.items()
        }
        
        # Sort topics by message rate (descending)
        sorted_topics = sorted(
            self.message_rates.items(), 
            key=lambda x: x[1], 
            reverse=True
        )
        
        # Return the top N topics with their message rates and partition counts
        return [
            (topic, rate, topic_partition_counts.get(topic, 0))
            for topic, rate in sorted_topics[:self.num_topics]
        ]

    def display_top_topics(self, top_topics: List[Tuple[str, float, int]]):
        """Display the top topics in a formatted table."""
        headers = ["Rank", "Topic", "Messages/sec", "Partitions"]
        table_data = []
        
        for i, (topic, rate, partition_count) in enumerate(top_topics, 1):
            table_data.append([i, topic, f"{rate:.2f}", partition_count])
        
        print(f"\n--- Top {self.num_topics} Kafka Topics by Production Rate ---")
        print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Broker(s): {self.bootstrap_servers}")
        print(tabulate(table_data, headers=headers, tablefmt="grid"))

    def run(self):
        """Run the monitoring loop."""
        try:
            print(f"Starting Kafka Top Topics monitor...")
            print(f"Monitoring {self.bootstrap_servers}")
            print(f"Press Ctrl+C to exit")
            
            # Initial offset collection
            self.collect_offsets()
            
            while True:
                # Wait for the refresh interval
                time.sleep(self.refresh_interval)
                
                # Collect current offsets
                self.collect_offsets()
                
                # Calculate message rates
                self.calculate_message_rates()
                
                # Get and display top topics
                top_topics = self.get_top_topics()
                self.display_top_topics(top_topics)
                
        except KeyboardInterrupt:
            print("\nShutting down...")
        finally:
            self.consumer.close()


def main():
    """Parse command line arguments and start the monitor."""
    parser = argparse.ArgumentParser(
        description="Monitor top Kafka topics by message production rate"
    )
    parser.add_argument(
        "--bootstrap-servers", 
        default="localhost:9092",
        help="Kafka bootstrap servers (comma-separated list)"
    )
    parser.add_argument(
        "--num-topics", 
        type=int, 
        default=10,
        help="Number of top topics to display"
    )
    parser.add_argument(
        "--refresh-interval", 
        type=int, 
        default=10,
        help="Refresh interval in seconds"
    )
    
    args = parser.parse_args()
    
    monitor = KafkaTopTopicsMonitor(
        bootstrap_servers=args.bootstrap_servers,
        num_topics=args.num_topics,
        refresh_interval=args.refresh_interval
    )
    
    monitor.run()


if __name__ == "__main__":
    main()


