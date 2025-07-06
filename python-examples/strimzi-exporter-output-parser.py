import re
import json
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from enum import Enum

class MetricType(Enum):
    GAUGE = "gauge"
    COUNTER = "counter"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"

@dataclass
class Metric:
    name: str
    help_text: str
    metric_type: MetricType
    labels: Dict[str, str]
    value: float
    timestamp: Optional[int] = None

class StrimziMetricsParser:
    def __init__(self):
        self.metrics: List[Metric] = []
        self.current_help = ""
        self.current_type = None
        self.current_name = ""
    
    def parse_prometheus_format(self, content: str) -> List[Metric]:
        """Parse Prometheus format metrics output"""
        self.metrics = []
        lines = content.strip().split('\n')
        
        for line in lines:
            line = line.strip()
            if not line or line.startswith('#'):
                self._parse_comment_line(line)
            else:
                self._parse_metric_line(line)
        
        return self.metrics
    
    def _parse_comment_line(self, line: str):
        """Parse comment lines for HELP and TYPE directives"""
        if line.startswith('# HELP'):
            parts = line.split(' ', 3)
            if len(parts) >= 4:
                self.current_name = parts[2]
                self.current_help = parts[3]
        elif line.startswith('# TYPE'):
            parts = line.split(' ', 3)
            if len(parts) >= 4:
                type_str = parts[3].lower()
                try:
                    self.current_type = MetricType(type_str)
                except ValueError:
                    self.current_type = MetricType.GAUGE
    
    def _parse_metric_line(self, line: str):
        """Parse a metric line and extract name, labels, and value"""
        # Regex to match metric format: metric_name{labels} value [timestamp]
        pattern = r'^([a-zA-Z_][a-zA-Z0-9_]*(?::[a-zA-Z_][a-zA-Z0-9_]*)*)\s*(\{[^}]*\})?\s+([^\s]+)(?:\s+(\d+))?'
        match = re.match(pattern, line)
        
        if not match:
            return
        
        metric_name = match.group(1)
        labels_str = match.group(2) or ""
        value_str = match.group(3)
        timestamp_str = match.group(4)
        
        # Parse labels
        labels = self._parse_labels(labels_str)
        
        # Parse value
        try:
            value = float(value_str)
        except ValueError:
            return
        
        # Parse timestamp if present
        timestamp = None
        if timestamp_str:
            try:
                timestamp = int(timestamp_str)
            except ValueError:
                pass
        
        # Create metric object
        metric = Metric(
            name=metric_name,
            help_text=self.current_help if metric_name.startswith(self.current_name) else "",
            metric_type=self.current_type or MetricType.GAUGE,
            labels=labels,
            value=value,
            timestamp=timestamp
        )
        
        self.metrics.append(metric)
    
    def _parse_labels(self, labels_str: str) -> Dict[str, str]:
        """Parse label string into dictionary"""
        labels = {}
        if not labels_str or labels_str == "{}":
            return labels
        
        # Remove outer braces
        labels_str = labels_str.strip('{}')
        
        # Split by comma, but handle quoted values
        pattern = r'([^,=]+)=([^,]+)(?:,|$)'
        matches = re.findall(pattern, labels_str)
        
        for key, value in matches:
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and value:
                labels[key] = value
        
        return labels
    
    def get_metrics_by_name(self, name: str) -> List[Metric]:
        """Get all metrics with a specific name"""
        return [m for m in self.metrics if m.name == name]
    
    def get_metrics_by_type(self, metric_type: MetricType) -> List[Metric]:
        """Get all metrics of a specific type"""
        return [m for m in self.metrics if m.metric_type == metric_type]
    
    def get_kafka_topic_metrics(self) -> Dict[str, List[Metric]]:
        """Get metrics grouped by Kafka topic"""
        topic_metrics = {}
        
        for metric in self.metrics:
            if 'topic' in metric.labels:
                topic = metric.labels['topic']
                if topic not in topic_metrics:
                    topic_metrics[topic] = []
                topic_metrics[topic].append(metric)
        
        return topic_metrics
    
    def get_strimzi_resource_states(self) -> List[Dict[str, Any]]:
        """Get Strimzi resource states"""
        states = []
        state_mapping = {0: "Unknown", 1: "NotReady", 2: "Ready"}
        
        for metric in self.get_metrics_by_name('strimzi_resource_state'):
            state_info = {
                'kind': metric.labels.get('kind', ''),
                'name': metric.labels.get('name', ''),
                'namespace': metric.labels.get('namespace', ''),
                'state_value': int(metric.value),
                'state_name': state_mapping.get(int(metric.value), 'Unknown')
            }
            states.append(state_info)
        
        return states
    
    def get_summary_stats(self) -> Dict[str, Any]:
        """Get summary statistics from parsed metrics"""
        stats = {
            'total_metrics': len(self.metrics),
            'metric_types': {},
            'unique_metric_names': set(),
            'brokers_with_metrics': set(),
            'topics_with_metrics': set()
        }
        
        for metric in self.metrics:
            # Count by type
            type_name = metric.metric_type.value
            stats['metric_types'][type_name] = stats['metric_types'].get(type_name, 0) + 1
            
            # Collect unique names
            stats['unique_metric_names'].add(metric.name)
            
            # Collect broker info if available
            if 'broker' in metric.labels:
                stats['brokers_with_metrics'].add(metric.labels['broker'])
            
            # Collect topic info if available
            if 'topic' in metric.labels and metric.labels['topic']:
                stats['topics_with_metrics'].add(metric.labels['topic'])
        
        # Convert sets to lists for JSON serialization
        stats['unique_metric_names'] = list(stats['unique_metric_names'])
        stats['brokers_with_metrics'] = list(stats['brokers_with_metrics'])
        stats['topics_with_metrics'] = list(stats['topics_with_metrics'])
        
        return stats
    
    def to_json(self) -> str:
        """Convert parsed metrics to JSON format"""
        metrics_data = []
        for metric in self.metrics:
            metrics_data.append({
                'name': metric.name,
                'help': metric.help_text,
                'type': metric.metric_type.value,
                'labels': metric.labels,
                'value': metric.value,
                'timestamp': metric.timestamp
            })
        
        return json.dumps(metrics_data, indent=2)

# Example usage
def main():
    # Sample Strimzi metrics data (you would read this from a file or HTTP endpoint)
    sample_metrics = '''
# HELP kafka_server_brokertopicmetrics_bytesinpersec Bytes in per second for broker topics
# TYPE kafka_server_brokertopicmetrics_bytesinpersec gauge
kafka_server_brokertopicmetrics_bytesinpersec{topic="user-events",} 1234.5
kafka_server_brokertopicmetrics_bytesinpersec{topic="order-events",} 2345.6

# HELP strimzi_resource_state State of Strimzi custom resources
# TYPE strimzi_resource_state gauge
strimzi_resource_state{kind="Kafka",name="my-cluster",namespace="kafka",} 2.0
strimzi_resource_state{kind="KafkaTopic",name="user-events",namespace="kafka",} 2.0
    '''
    
    # Parse the metrics
    parser = StrimziMetricsParser()
    metrics = parser.parse_prometheus_format(sample_metrics)
    
    # Print summary
    print("=== Parsing Summary ===")
    summary = parser.get_summary_stats()
    print(f"Total metrics: {summary['total_metrics']}")
    print(f"Metric types: {summary['metric_types']}")
    print(f"Topics: {summary['topics_with_metrics']}")
    
    # Get topic-specific metrics
    print("\n=== Topic Metrics ===")
    topic_metrics = parser.get_kafka_topic_metrics()
    for topic, metrics_list in topic_metrics.items():
        print(f"Topic '{topic}': {len(metrics_list)} metrics")
        for metric in metrics_list:
            print(f"  {metric.name}: {metric.value}")
    
    # Get Strimzi resource states
    print("\n=== Strimzi Resource States ===")
    resource_states = parser.get_strimzi_resource_states()
    for resource in resource_states:
        print(f"{resource['kind']}/{resource['name']}: {resource['state_name']}")
    
    # Export to JSON
    print("\n=== JSON Export (first 500 chars) ===")
    json_output = parser.to_json()
    print(json_output[:500] + "..." if len(json_output) > 500 else json_output)

if __name__ == "__main__":
    main()
