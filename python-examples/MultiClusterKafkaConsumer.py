import threading
import time
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import json
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MultiClusterKafkaConsumer:
    def __init__(self):
        # Define your Kafka clusters configuration
        self.clusters = {
            'cluster1': {
                'bootstrap_servers': ['localhost:9092', 'localhost:9093'],
                'topics': ['topic1', 'topic2'],
                'group_id': 'consumer_group_cluster1',
                'auto_offset_reset': 'earliest'
            },
            'cluster2': {
                'bootstrap_servers': ['kafka2.example.com:9092', 'kafka2.example.com:9093'],
                'topics': ['orders', 'payments'],
                'group_id': 'consumer_group_cluster2',
                'auto_offset_reset': 'latest'
            },
            'cluster3': {
                'bootstrap_servers': ['kafka3.example.com:9092'],
                'topics': ['logs', 'metrics'],
                'group_id': 'consumer_group_cluster3',
                'auto_offset_reset': 'earliest'
            }
        }
        
        self.consumers = {}
        self.consumer_threads = {}
        self.running = True

    def create_consumer(self, cluster_name, config):
        """Create a KafkaConsumer for a specific cluster"""
        try:
            consumer = KafkaConsumer(
                *config['topics'],
                bootstrap_servers=config['bootstrap_servers'],
                group_id=config['group_id'],
                auto_offset_reset=config['auto_offset_reset'],
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,
                value_deserializer=lambda x: x.decode('utf-8') if x else None,
                key_deserializer=lambda x: x.decode('utf-8') if x else None,
                consumer_timeout_ms=1000,  # Timeout to allow checking self.running
                max_poll_records=500,
                session_timeout_ms=30000,
                heartbeat_interval_ms=3000
            )
            
            logger.info(f"Created consumer for {cluster_name}")
            return consumer
            
        except Exception as e:
            logger.error(f"Failed to create consumer for {cluster_name}: {e}")
            return None

    def consume_messages(self, cluster_name, consumer):
        """Consume messages from a specific cluster"""
        logger.info(f"Starting message consumption for {cluster_name}")
        
        while self.running:
            try:
                # Poll for messages with timeout
                message_batch = consumer.poll(timeout_ms=1000)
                
                if message_batch:
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            self.process_message(cluster_name, message)
                
            except KafkaError as e:
                logger.error(f"Kafka error in {cluster_name}: {e}")
                time.sleep(5)  # Wait before retrying
                
            except Exception as e:
                logger.error(f"Unexpected error in {cluster_name}: {e}")
                time.sleep(5)
        
        logger.info(f"Stopping consumer for {cluster_name}")
        consumer.close()

    def process_message(self, cluster_name, message):
        """Process individual message - customize this method based on your needs"""
        try:
            logger.info(f"[{cluster_name}] Topic: {message.topic}, "
                       f"Partition: {message.partition}, "
                       f"Offset: {message.offset}")
            
            # Try to parse JSON if possible
            try:
                if message.value:
                    data = json.loads(message.value)
                    logger.info(f"[{cluster_name}] Message data: {data}")
                else:
                    logger.info(f"[{cluster_name}] Empty message")
            except json.JSONDecodeError:
                logger.info(f"[{cluster_name}] Message (raw): {message.value}")
            
            # Add your custom message processing logic here
            self.handle_business_logic(cluster_name, message)
            
        except Exception as e:
            logger.error(f"Error processing message from {cluster_name}: {e}")

    def handle_business_logic(self, cluster_name, message):
        """Handle business logic based on cluster and message content"""
        # Example routing based on cluster
        if cluster_name == 'cluster1':
            self.handle_cluster1_message(message)
        elif cluster_name == 'cluster2':
            self.handle_cluster2_message(message)
        elif cluster_name == 'cluster3':
            self.handle_cluster3_message(message)

    def handle_cluster1_message(self, message):
        """Handle messages from cluster 1"""
        logger.info(f"Processing cluster1 message from topic {message.topic}")
        # Add your specific logic for cluster 1

    def handle_cluster2_message(self, message):
        """Handle messages from cluster 2"""
        logger.info(f"Processing cluster2 message from topic {message.topic}")
        # Add your specific logic for cluster 2

    def handle_cluster3_message(self, message):
        """Handle messages from cluster 3"""
        logger.info(f"Processing cluster3 message from topic {message.topic}")
        # Add your specific logic for cluster 3

    def start_consumers(self):
        """Start consumers for all clusters in separate threads"""
        for cluster_name, config in self.clusters.items():
            consumer = self.create_consumer(cluster_name, config)
            
            if consumer:
                self.consumers[cluster_name] = consumer
                
                # Create and start thread for this consumer
                thread = threading.Thread(
                    target=self.consume_messages,
                    args=(cluster_name, consumer),
                    name=f"Consumer-{cluster_name}"
                )
                thread.daemon = True
                self.consumer_threads[cluster_name] = thread
                thread.start()
                
                logger.info(f"Started consumer thread for {cluster_name}")
            else:
                logger.error(f"Failed to start consumer for {cluster_name}")

    def stop_consumers(self):
        """Stop all consumers gracefully"""
        logger.info("Stopping all consumers...")
        self.running = False
        
        # Wait for all threads to finish
        for cluster_name, thread in self.consumer_threads.items():
            logger.info(f"Waiting for {cluster_name} consumer to stop...")
            thread.join(timeout=10)
            
        logger.info("All consumers stopped")

    def get_consumer_status(self):
        """Get status of all consumers"""
        status = {}
        for cluster_name, thread in self.consumer_threads.items():
            status[cluster_name] = {
                'thread_alive': thread.is_alive(),
                'consumer_active': cluster_name in self.consumers
            }
        return status


def main():
    """Main function to run the multi-cluster consumer"""
    consumer_manager = MultiClusterKafkaConsumer()
    
    try:
        # Start all consumers
        consumer_manager.start_consumers()
        
        logger.info("All consumers started. Press Ctrl+C to stop...")
        
        # Keep the main thread alive and monitor consumers
        while True:
            time.sleep(10)
            
            # Optional: Check consumer status periodically
            status = consumer_manager.get_consumer_status()
            active_consumers = sum(1 for s in status.values() if s['thread_alive'])
            logger.info(f"Active consumers: {active_consumers}/{len(status)}")
            
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    except Exception as e:
        logger.error(f"Unexpected error in main: {e}")
    finally:
        consumer_manager.stop_consumers()


if __name__ == "__main__":
    main()