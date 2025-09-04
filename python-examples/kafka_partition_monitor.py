#!/usr/bin/env python3
"""
Kafka Under-Replicated and Unavailable Partitions Monitor

This script monitors Kafka clusters for:
- Under-replicated partitions (URPs)
- Unavailable partitions
- Offline partitions
- General partition health metrics

Requirements:
pip install kafka-python
"""

import json
import time
import logging
from datetime import datetime
from typing import Dict, List, Tuple, Set
from collections import defaultdict

from kafka import KafkaAdminClient, KafkaConsumer
from kafka.admin.config_resource import ConfigResource, ConfigResourceType
from kafka.errors import KafkaError


class KafkaPartitionMonitor:
    def __init__(self, bootstrap_servers: str, client_id: str = "partition-monitor"):
        """
        Initialize Kafka Partition Monitor
        
        Args:
            bootstrap_servers: Comma-separated list of Kafka brokers
            client_id: Client ID for Kafka connections
        """
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        
        # Initialize Kafka Admin Client
        self.admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id=client_id,
            request_timeout_ms=10000,
            api_version=(2, 0, 0)
        )
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def get_cluster_metadata(self) -> Dict:
        """Get cluster metadata including brokers and topics"""
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                api_version=(2, 0, 0)
            )
            
            metadata = consumer.list_consumer_groups()
            cluster_metadata = consumer._client.cluster
            
            brokers = {}
            for broker in cluster_metadata.brokers():
                brokers[broker.nodeId] = {
                    'id': broker.nodeId,
                    'host': broker.host,
                    'port': broker.port,
                    'rack': getattr(broker, 'rack', None)
                }
            
            consumer.close()
            
            return {
                'brokers': brokers,
                'controller': cluster_metadata.controller.nodeId if cluster_metadata.controller else None
            }
            
        except Exception as e:
            self.logger.error(f"Error getting cluster metadata: {e}")
            return {'brokers': {}, 'controller': None}

    def get_topic_partitions(self) -> Dict[str, List[Dict]]:
        """Get all topics and their partition information"""
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                api_version=(2, 0, 0)
            )
            
            # Get topic metadata
            topics_metadata = consumer.list_consumer_group_offsets()
            cluster = consumer._client.cluster
            
            topics_info = {}
            
            for topic in cluster.topics():
                if topic.startswith('__'):  # Skip internal topics
                    continue
                    
                partitions = []
                topic_partitions = cluster.partitions_for_topic(topic)
                
                if topic_partitions:
                    for partition_id in topic_partitions:
                        partition_metadata = cluster.partition_metadata(topic, partition_id)
                        
                        partition_info = {
                            'partition': partition_id,
                            'leader': partition_metadata.leader,
                            'replicas': list(partition_metadata.replicas),
                            'isr': list(partition_metadata.isr),
                            'is_available': partition_metadata.leader is not None,
                            'is_under_replicated': len(partition_metadata.isr) < len(partition_metadata.replicas),
                            'replication_factor': len(partition_metadata.replicas)
                        }
                        partitions.append(partition_info)
                
                topics_info[topic] = partitions
            
            consumer.close()
            return topics_info
            
        except Exception as e:
            self.logger.error(f"Error getting topic partitions: {e}")
            return {}

    def analyze_partition_health(self) -> Dict:
        """Analyze partition health and return detailed report"""
        topics_info = self.get_topic_partitions()
        cluster_metadata = self.get_cluster_metadata()
        
        analysis = {
            'timestamp': datetime.now().isoformat(),
            'cluster_info': cluster_metadata,
            'total_topics': len(topics_info),
            'total_partitions': 0,
            'under_replicated_partitions': [],
            'unavailable_partitions': [],
            'healthy_partitions': 0,
            'summary': {
                'total_urp': 0,
                'total_unavailable': 0,
                'topics_with_issues': set(),
                'affected_brokers': set()
            }
        }
        
        for topic, partitions in topics_info.items():
            analysis['total_partitions'] += len(partitions)
            
            for partition in partitions:
                partition_info = {
                    'topic': topic,
                    'partition': partition['partition'],
                    'leader': partition['leader'],
                    'replicas': partition['replicas'],
                    'isr': partition['isr'],
                    'replication_factor': partition['replication_factor']
                }
                
                # Check if partition is unavailable
                if not partition['is_available']:
                    analysis['unavailable_partitions'].append(partition_info)
                    analysis['summary']['total_unavailable'] += 1
                    analysis['summary']['topics_with_issues'].add(topic)
                    
                # Check if partition is under-replicated
                elif partition['is_under_replicated']:
                    partition_info['missing_replicas'] = len(partition['replicas']) - len(partition['isr'])
                    partition_info['offline_replicas'] = [r for r in partition['replicas'] if r not in partition['isr']]
                    
                    analysis['under_replicated_partitions'].append(partition_info)
                    analysis['summary']['total_urp'] += 1
                    analysis['summary']['topics_with_issues'].add(topic)
                    analysis['summary']['affected_brokers'].update(partition_info['offline_replicas'])
                    
                else:
                    analysis['healthy_partitions'] += 1
        
        # Convert sets to lists for JSON serialization
        analysis['summary']['topics_with_issues'] = list(analysis['summary']['topics_with_issues'])
        analysis['summary']['affected_brokers'] = list(analysis['summary']['affected_brokers'])
        
        return analysis

    def print_health_report(self, analysis: Dict):
        """Print a formatted health report"""
        print("\n" + "="*80)
        print(f"KAFKA CLUSTER HEALTH REPORT - {analysis['timestamp']}")
        print("="*80)
        
        # Cluster Summary
        print(f"\nCLUSTER SUMMARY:")
        print(f"  Total Topics: {analysis['total_topics']}")
        print(f"  Total Partitions: {analysis['total_partitions']}")
        print(f"  Healthy Partitions: {analysis['healthy_partitions']}")
        print(f"  Under-Replicated Partitions: {analysis['summary']['total_urp']}")
        print(f"  Unavailable Partitions: {analysis['summary']['total_unavailable']}")
        
        # Broker Information
        print(f"\nBROKER INFORMATION:")
        for broker_id, broker in analysis['cluster_info']['brokers'].items():
            status = "AFFECTED" if broker_id in analysis['summary']['affected_brokers'] else "OK"
            print(f"  Broker {broker_id}: {broker['host']}:{broker['port']} [{status}]")
        
        # Under-Replicated Partitions
        if analysis['under_replicated_partitions']:
            print(f"\nUNDER-REPLICATED PARTITIONS ({len(analysis['under_replicated_partitions'])}):")
            print("-" * 80)
            for urp in analysis['under_replicated_partitions']:
                print(f"  Topic: {urp['topic']}, Partition: {urp['partition']}")
                print(f"    Leader: {urp['leader']}")
                print(f"    Replicas: {urp['replicas']} (RF: {urp['replication_factor']})")
                print(f"    ISR: {urp['isr']}")
                print(f"    Missing: {urp['missing_replicas']} replicas")
                print(f"    Offline Brokers: {urp['offline_replicas']}")
                print()
        
        # Unavailable Partitions
        if analysis['unavailable_partitions']:
            print(f"\nUNAVAILABLE PARTITIONS ({len(analysis['unavailable_partitions'])}):")
            print("-" * 80)
            for up in analysis['unavailable_partitions']:
                print(f"  Topic: {up['topic']}, Partition: {up['partition']}")
                print(f"    Leader: NONE (OFFLINE)")
                print(f"    Replicas: {up['replicas']}")
                print(f"    ISR: {up['isr']}")
                print()
        
        # Health Status
        health_status = "HEALTHY" if (analysis['summary']['total_urp'] == 0 and 
                                    analysis['summary']['total_unavailable'] == 0) else "UNHEALTHY"
        print(f"\nOVERALL CLUSTER HEALTH: {health_status}")
        print("="*80)

    def save_report_to_file(self, analysis: Dict, filename: str = None):
        """Save the analysis report to a JSON file"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"kafka_partition_health_{timestamp}.json"
        
        try:
            with open(filename, 'w') as f:
                json.dump(analysis, f, indent=2)
            self.logger.info(f"Report saved to {filename}")
        except Exception as e:
            self.logger.error(f"Error saving report to file: {e}")

    def monitor_continuous(self, interval: int = 60):
        """Continuously monitor partition health"""
        print(f"Starting continuous monitoring (interval: {interval} seconds)")
        print("Press Ctrl+C to stop...")
        
        try:
            while True:
                analysis = self.analyze_partition_health()
                
                # Print summary
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                urp_count = analysis['summary']['total_urp']
                unavailable_count = analysis['summary']['total_unavailable']
                
                print(f"[{timestamp}] URPs: {urp_count}, Unavailable: {unavailable_count}")
                
                # Alert if issues found
                if urp_count > 0 or unavailable_count > 0:
                    print("⚠️  ISSUES DETECTED!")
                    self.print_health_report(analysis)
                    
                    # Save detailed report
                    self.save_report_to_file(analysis)
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print("\nMonitoring stopped by user")
        except Exception as e:
            self.logger.error(f"Error in continuous monitoring: {e}")


def main():
    """Main function with example usage"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Kafka Partition Health Monitor")
    parser.add_argument("--brokers", default="localhost:9092",
                      help="Kafka bootstrap servers (default: localhost:9092)")
    parser.add_argument("--continuous", action="store_true",
                      help="Run continuous monitoring")
    parser.add_argument("--interval", type=int, default=60,
                      help="Monitoring interval in seconds (default: 60)")
    parser.add_argument("--save", action="store_true",
                      help="Save report to JSON file")
    
    args = parser.parse_args()
    
    # Initialize monitor
    monitor = KafkaPartitionMonitor(args.brokers)
    
    try:
        if args.continuous:
            monitor.monitor_continuous(args.interval)
        else:
            # Single analysis
            analysis = monitor.analyze_partition_health()
            monitor.print_health_report(analysis)
            
            if args.save:
                monitor.save_report_to_file(analysis)
                
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
