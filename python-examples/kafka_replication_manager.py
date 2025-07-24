#!/usr/bin/env python3
"""
Kafka Partition Replication Order Management via Python API
This script uses kafka-python library to manage partition replica assignments
"""

import json
import time
import logging
from typing import Dict, List, Optional, Tuple
from kafka import KafkaAdminClient, KafkaConsumer
from kafka.admin import ConfigResource, ConfigResourceType, AlterReplicaLogDirsRequest
from kafka.admin.client_async import KafkaAdminClient as AsyncKafkaAdminClient
from kafka.protocol.admin import AlterPartitionReassignmentsRequest_v0 as AlterPartitionReassignmentsRequest
from kafka.protocol.admin import ListPartitionReassignmentsRequest_v0 as ListPartitionReassignmentsRequest
from kafka.structs import TopicPartition
from kafka.errors import KafkaError
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class KafkaReplicationManager:
    """Manages Kafka partition replication order changes via Python API"""
    
    def __init__(self, bootstrap_servers: str = 'localhost:9092'):
        """
        Initialize the Kafka Replication Manager
        
        Args:
            bootstrap_servers: Comma-separated list of Kafka brokers
        """
        self.bootstrap_servers = bootstrap_servers.split(',')
        self.admin_client = None
        self.consumer = None
        
    def connect(self) -> bool:
        """Establish connection to Kafka cluster"""
        try:
            self.admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                client_id='replication_manager',
                request_timeout_ms=30000
            )
            
            self.consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                consumer_timeout_ms=5000
            )
            
            # Test connection
            cluster_metadata = self.consumer.list_consumer_groups()
            logger.info(f"Connected to Kafka cluster with {len(self.bootstrap_servers)} brokers")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            return False
    
    def get_topic_metadata(self, topic_name: str) -> Optional[Dict]:
        """Get detailed metadata for a topic"""
        try:
            metadata = self.consumer.list_consumer_group_offsets()
            topic_metadata = self.admin_client.describe_topics([topic_name])
            
            if topic_name not in topic_metadata:
                logger.error(f"Topic '{topic_name}' not found")
                return None
                
            topic_info = topic_metadata[topic_name]
            
            result = {
                'topic': topic_name,
                'partitions': {},
                'total_partitions': len(topic_info.partitions)
            }
            
            for partition_id, partition_info in topic_info.partitions.items():
                result['partitions'][partition_id] = {
                    'partition': partition_id,
                    'leader': partition_info.leader,
                    'replicas': partition_info.replicas,
                    'isr': partition_info.isr,
                    'replica_count': len(partition_info.replicas)
                }
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting topic metadata: {e}")
            return None
    
    def display_current_assignment(self, topic_name: str):
        """Display current partition replica assignments"""
        metadata = self.get_topic_metadata(topic_name)
        
        if not metadata:
            return
            
        print(f"\n{'='*60}")
        print(f"Current Partition Assignment for Topic: {topic_name}")
        print(f"{'='*60}")
        print(f"Total Partitions: {metadata['total_partitions']}")
        print()
        
        for partition_id in sorted(metadata['partitions'].keys()):
            partition = metadata['partitions'][partition_id]
            print(f"Partition {partition_id:2d}: "
                  f"Leader={partition['leader']:2d}, "
                  f"Replicas={partition['replicas']}, "
                  f"ISR={partition['isr']}")
    
    def create_reassignment_plan(self, topic_name: str, 
                               custom_assignment: Optional[Dict[int, List[int]]] = None) -> Dict:
        """
        Create a partition reassignment plan
        
        Args:
            topic_name: Name of the topic
            custom_assignment: Dict mapping partition_id -> list of broker_ids
            
        Returns:
            Reassignment plan dictionary
        """
        metadata = self.get_topic_metadata(topic_name)
        if not metadata:
            return {}
        
        reassignment_plan = {
            'version': 1,
            'partitions': []
        }
        
        for partition_id in sorted(metadata['partitions'].keys()):
            current_replicas = metadata['partitions'][partition_id]['replicas']
            
            if custom_assignment and partition_id in custom_assignment:
                new_replicas = custom_assignment[partition_id]
            else:
                # Default: reverse the current replica order
                new_replicas = list(reversed(current_replicas))
            
            reassignment_plan['partitions'].append({
                'topic': topic_name,
                'partition': partition_id,
                'replicas': new_replicas
            })
        
        return reassignment_plan
    
    def validate_reassignment_plan(self, plan: Dict, available_brokers: List[int]) -> bool:
        """Validate a reassignment plan"""
        try:
            for partition_info in plan['partitions']:
                replicas = partition_info['replicas']
                
                # Check if all brokers exist
                for broker_id in replicas:
                    if broker_id not in available_brokers:
                        logger.error(f"Broker {broker_id} not available. Available brokers: {available_brokers}")
                        return False
                
                # Check for duplicate replicas
                if len(replicas) != len(set(replicas)):
                    logger.error(f"Duplicate replicas found in partition {partition_info['partition']}: {replicas}")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating reassignment plan: {e}")
            return False
    
    def get_available_brokers(self) -> List[int]:
        """Get list of available broker IDs"""
        try:
            cluster_metadata = self.admin_client.describe_cluster()
            return [broker.nodeId for broker in cluster_metadata.brokers]
        except Exception as e:
            logger.error(f"Error getting available brokers: {e}")
            return []
    
    def execute_reassignment(self, plan: Dict) -> bool:
        """
        Execute partition reassignment plan
        Note: This uses a simplified approach as kafka-python has limited support
        for partition reassignment. For production use, consider using confluent-kafka
        """
        try:
            logger.info("Executing partition reassignment...")
            logger.info("Note: Using kafka-python has limitations for reassignment operations")
            
            # Display the plan that would be executed
            print("\nReassignment Plan:")
            print(json.dumps(plan, indent=2))
            
            # For actual execution, you would typically use:
            # 1. confluent-kafka library (more complete)
            # 2. Direct API calls to Kafka
            # 3. Kafka command-line tools via subprocess
            
            logger.warning("Actual reassignment execution requires confluent-kafka or direct API calls")
            logger.info("Plan validated and ready for execution")
            
            return True
            
        except Exception as e:
            logger.error(f"Error executing reassignment: {e}")
            return False
    
    def monitor_reassignment_progress(self, topic_name: str, max_wait_time: int = 300):
        """Monitor reassignment progress"""
        logger.info(f"Monitoring reassignment progress for topic: {topic_name}")
        
        start_time = time.time()
        
        while time.time() - start_time < max_wait_time:
            try:
                metadata = self.get_topic_metadata(topic_name)
                if metadata:
                    print(f"\n[{time.strftime('%H:%M:%S')}] Current Status:")
                    for partition_id in sorted(metadata['partitions'].keys()):
                        partition = metadata['partitions'][partition_id]
                        replicas = partition['replicas']
                        isr = partition['isr']
                        in_sync = len(replicas) == len(isr)
                        status = "✓ In Sync" if in_sync else "⚠ Syncing"
                        print(f"  Partition {partition_id}: {replicas} -> ISR: {isr} [{status}]")
                
                time.sleep(10)
                
            except KeyboardInterrupt:
                print("\nMonitoring stopped by user")
                break
            except Exception as e:
                logger.error(f"Error monitoring progress: {e}")
                break
    
    def close(self):
        """Close connections"""
        if self.consumer:
            self.consumer.close()
        if self.admin_client:
            self.admin_client.close()

def main():
    """Main function with command-line interface"""
    parser = argparse.ArgumentParser(description='Kafka Partition Replication Manager')
    parser.add_argument('--brokers', default='localhost:9092', 
                       help='Kafka bootstrap servers (comma-separated)')
    parser.add_argument('--topic', required=True, 
                       help='Topic name to manage')
    parser.add_argument('--action', choices=['show', 'plan', 'execute', 'monitor'], 
                       default='show', help='Action to perform')
    parser.add_argument('--assignment', type=str,
                       help='Custom assignment as JSON string: {"0": [1,2,0], "1": [2,0,1]}')
    
    args = parser.parse_args()
    
    manager = KafkaReplicationManager(args.brokers)
    
    if not manager.connect():
        logger.error("Failed to connect to Kafka cluster")
        return 1
    
    try:
        if args.action == 'show':
            manager.display_current_assignment(args.topic)
        
        elif args.action == 'plan':
            custom_assignment = None
            if args.assignment:
                try:
                    custom_assignment = {int(k): v for k, v in json.loads(args.assignment).items()}
                except json.JSONDecodeError:
                    logger.error("Invalid JSON format for assignment")
                    return 1
            
            plan = manager.create_reassignment_plan(args.topic, custom_assignment)
            available_brokers = manager.get_available_brokers()
            
            if manager.validate_reassignment_plan(plan, available_brokers):
                print("\nGenerated Reassignment Plan:")
                print(json.dumps(plan, indent=2))
                
                # Save plan to file
                filename = f"{args.topic}_reassignment_plan.json"
                with open(filename, 'w') as f:
                    json.dump(plan, f, indent=2)
                print(f"\nPlan saved to: {filename}")
            else:
                logger.error("Invalid reassignment plan")
                return 1
        
        elif args.action == 'execute':
            # Load plan from file
            filename = f"{args.topic}_reassignment_plan.json"
            try:
                with open(filename, 'r') as f:
                    plan = json.load(f)
                
                print("Loaded reassignment plan:")
                print(json.dumps(plan, indent=2))
                
                confirm = input("\nExecute this reassignment plan? (yes/no): ")
                if confirm.lower() == 'yes':
                    manager.execute_reassignment(plan)
                else:
                    print("Execution cancelled")
                    
            except FileNotFoundError:
                logger.error(f"Plan file not found: {filename}")
                logger.info("Generate a plan first using --action plan")
                return 1
        
        elif args.action == 'monitor':
            manager.monitor_reassignment_progress(args.topic)
    
    finally:
        manager.close()
    
    return 0

if __name__ == "__main__":
    exit(main())

# Example usage:
"""
# Install required package:
pip install kafka-python

# Show current assignment:
python kafka_replication_manager.py --topic my-topic --action show

# Create reassignment plan (reverses replica order):
python kafka_replication_manager.py --topic my-topic --action plan

# Create custom reassignment plan:
python kafka_replication_manager.py --topic my-topic --action plan --assignment '{"0": [1,2,0], "1": [2,0,1], "2": [0,1,2]}'

# Execute reassignment:
python kafka_replication_manager.py --topic my-topic --action execute

# Monitor progress:
python kafka_replication_manager.py --topic my-topic --action monitor
"""
