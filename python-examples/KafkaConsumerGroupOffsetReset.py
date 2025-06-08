#!/usr/bin/env python3
"""
Kafka Consumer Group Offset Reset Script

This script provides functionality to reset Kafka consumer group offsets
using the kafka-python library.
"""

import argparse
import sys
from kafka import KafkaAdminClient, KafkaConsumer
from kafka.structs import TopicPartition, OffsetAndMetadata
from kafka.errors import KafkaError
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class KafkaOffsetResetManager:
    def __init__(self, bootstrap_servers):
        """Initialize the Kafka offset reset manager."""
        self.bootstrap_servers = bootstrap_servers
        self.admin_client = None
        self.consumer = None
        
    def connect(self):
        """Establish connection to Kafka cluster."""
        try:
            self.admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                client_id='offset-reset-admin'
            )
            self.consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                group_id='temp-offset-reset-group',
                enable_auto_commit=False
            )
            logger.info(f"Connected to Kafka cluster: {self.bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise
    
    def get_consumer_groups(self):
        """List all consumer groups."""
        try:
            groups = self.admin_client.list_consumer_groups()
            return [group[0] for group in groups]
        except Exception as e:
            logger.error(f"Failed to list consumer groups: {e}")
            return []
    
    def get_group_offsets(self, group_id):
        """Get current offsets for a consumer group."""
        try:
            offsets = self.admin_client.list_consumer_group_offsets(group_id)
            return offsets
        except Exception as e:
            logger.error(f"Failed to get offsets for group {group_id}: {e}")
            return {}
    
    def reset_to_earliest(self, group_id, topics=None):
        """Reset offsets to earliest available."""
        try:
            # Get current offsets to determine partitions
            current_offsets = self.get_group_offsets(group_id)
            
            if not current_offsets:
                logger.warning(f"No offsets found for group {group_id}")
                return False
            
            # Filter by topics if specified
            partitions_to_reset = []
            for tp in current_offsets.keys():
                if topics is None or tp.topic in topics:
                    partitions_to_reset.append(tp)
            
            if not partitions_to_reset:
                logger.warning("No partitions to reset")
                return False
            
            # Get earliest offsets
            earliest_offsets = self.consumer.beginning_offsets(partitions_to_reset)
            
            # Prepare offset reset data
            offset_data = {}
            for tp in partitions_to_reset:
                offset_data[tp] = OffsetAndMetadata(earliest_offsets[tp], None)
            
            # Reset offsets
            self.admin_client.alter_consumer_group_offsets(group_id, offset_data)
            logger.info(f"Successfully reset {len(offset_data)} partitions to earliest for group {group_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to reset offsets to earliest: {e}")
            return False
    
    def reset_to_latest(self, group_id, topics=None):
        """Reset offsets to latest available."""
        try:
            current_offsets = self.get_group_offsets(group_id)
            
            if not current_offsets:
                logger.warning(f"No offsets found for group {group_id}")
                return False
            
            # Filter by topics if specified
            partitions_to_reset = []
            for tp in current_offsets.keys():
                if topics is None or tp.topic in topics:
                    partitions_to_reset.append(tp)
            
            if not partitions_to_reset:
                logger.warning("No partitions to reset")
                return False
            
            # Get latest offsets
            latest_offsets = self.consumer.end_offsets(partitions_to_reset)
            
            # Prepare offset reset data
            offset_data = {}
            for tp in partitions_to_reset:
                offset_data[tp] = OffsetAndMetadata(latest_offsets[tp], None)
            
            # Reset offsets
            self.admin_client.alter_consumer_group_offsets(group_id, offset_data)
            logger.info(f"Successfully reset {len(offset_data)} partitions to latest for group {group_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to reset offsets to latest: {e}")
            return False
    
    def reset_to_offset(self, group_id, topic, partition, offset):
        """Reset specific partition to a specific offset."""
        try:
            tp = TopicPartition(topic, partition)
            offset_data = {tp: OffsetAndMetadata(offset, None)}
            
            self.admin_client.alter_consumer_group_offsets(group_id, offset_data)
            logger.info(f"Successfully reset {topic}:{partition} to offset {offset} for group {group_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to reset offset: {e}")
            return False
    
    def display_current_offsets(self, group_id):
        """Display current offsets for a consumer group."""
        try:
            offsets = self.get_group_offsets(group_id)
            
            if not offsets:
                logger.info(f"No offsets found for group {group_id}")
                return
            
            print(f"\nCurrent offsets for consumer group '{group_id}':")
            print("-" * 60)
            print(f"{'Topic':<30} {'Partition':<10} {'Offset':<10}")
            print("-" * 60)
            
            for tp, offset_metadata in sorted(offsets.items()):
                print(f"{tp.topic:<30} {tp.partition:<10} {offset_metadata.offset:<10}")
            
        except Exception as e:
            logger.error(f"Failed to display offsets: {e}")
    
    def close(self):
        """Close connections."""
        if self.consumer:
            self.consumer.close()
        if self.admin_client:
            self.admin_client.close()


def main():
    parser = argparse.ArgumentParser(description='Kafka Consumer Group Offset Reset Tool')
    parser.add_argument('--bootstrap-servers', required=True,
                       help='Kafka bootstrap servers (comma-separated)')
    parser.add_argument('--group-id', required=True,
                       help='Consumer group ID')
    parser.add_argument('--action', choices=['earliest', 'latest', 'specific', 'show'],
                       required=True, help='Reset action to perform')
    parser.add_argument('--topics', nargs='*',
                       help='Topics to reset (if not specified, all topics for the group)')
    parser.add_argument('--topic', help='Topic name (for specific offset reset)')
    parser.add_argument('--partition', type=int, help='Partition number (for specific offset reset)')
    parser.add_argument('--offset', type=int, help='Specific offset value')
    
    args = parser.parse_args()
    
    # Validate specific offset arguments
    if args.action == 'specific':
        if not all([args.topic, args.partition is not None, args.offset is not None]):
            logger.error("For specific offset reset, --topic, --partition, and --offset are required")
            sys.exit(1)
    
    # Initialize offset manager
    manager = KafkaOffsetResetManager(args.bootstrap_servers.split(','))
    
    try:
        manager.connect()
        
        if args.action == 'show':
            manager.display_current_offsets(args.group_id)
        elif args.action == 'earliest':
            success = manager.reset_to_earliest(args.group_id, args.topics)
            if success:
                print(f"Successfully reset offsets to earliest for group '{args.group_id}'")
        elif args.action == 'latest':
            success = manager.reset_to_latest(args.group_id, args.topics)
            if success:
                print(f"Successfully reset offsets to latest for group '{args.group_id}'")
        elif args.action == 'specific':
            success = manager.reset_to_offset(args.group_id, args.topic, args.partition, args.offset)
            if success:
                print(f"Successfully reset offset for group '{args.group_id}'")
        
        # Show updated offsets
        if args.action != 'show':
            print("\nUpdated offsets:")
            manager.display_current_offsets(args.group_id)
            
    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
    except Exception as e:
        logger.error(f"Operation failed: {e}")
        sys.exit(1)
    finally:
        manager.close()


if __name__ == '__main__':
    main()