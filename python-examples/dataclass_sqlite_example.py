import sqlite3
from dataclasses import dataclass
from typing import List, Optional, Dict
from datetime import datetime
from enum import Enum

class TopicStatus(Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    DEPRECATED = "deprecated"

@dataclass
class KafkaTopicMetadata:
    topic_name: str
    partitions: int
    replication_factor: int
    status: TopicStatus
    description: str
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    id: Optional[int] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()
        if self.updated_at is None:
            self.updated_at = datetime.now()

@dataclass
class KafkaOffset:
    topic_name: str
    partition: int
    consumer_group: str
    current_offset: int
    high_water_mark: int
    lag: int
    last_updated: Optional[datetime] = None
    id: Optional[int] = None
    
    def __post_init__(self):
        if self.last_updated is None:
            self.last_updated = datetime.now()
        # Calculate lag if not provided
        if self.lag == 0:
            self.lag = max(0, self.high_water_mark - self.current_offset)

@dataclass
class ConsumerGroupInfo:
    group_id: str
    state: str  # e.g., 'Stable', 'Dead', 'Empty', 'PreparingRebalance'
    protocol: str
    protocol_type: str
    members_count: int
    coordinator: str
    created_at: Optional[datetime] = None
    id: Optional[int] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()

class KafkaMetadataDatabase:
    def __init__(self, db_path: str = "kafka_metadata.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Initialize the database and create tables if they don't exist."""
        with sqlite3.connect(self.db_path) as conn:
            # Topic metadata table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS topic_metadata (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    topic_name TEXT UNIQUE NOT NULL,
                    partitions INTEGER NOT NULL,
                    replication_factor INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    description TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Offset information table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS kafka_offsets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    topic_name TEXT NOT NULL,
                    partition INTEGER NOT NULL,
                    consumer_group TEXT NOT NULL,
                    current_offset INTEGER NOT NULL,
                    high_water_mark INTEGER NOT NULL,
                    lag INTEGER NOT NULL,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(topic_name, partition, consumer_group)
                )
            """)
            
            # Consumer group information table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS consumer_groups (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_id TEXT UNIQUE NOT NULL,
                    state TEXT NOT NULL,
                    protocol TEXT,
                    protocol_type TEXT,
                    members_count INTEGER DEFAULT 0,
                    coordinator TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.commit()
    
    # Topic Metadata Operations
    def insert_topic_metadata(self, topic: KafkaTopicMetadata) -> int:
        """Insert topic metadata into the database."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                INSERT OR REPLACE INTO topic_metadata 
                (topic_name, partitions, replication_factor, status, description, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (topic.topic_name, topic.partitions, topic.replication_factor, 
                  topic.status.value, topic.description, topic.created_at, topic.updated_at))
            conn.commit()
            topic.id = cursor.lastrowid
            return cursor.lastrowid
    
    def get_topic_metadata(self, topic_name: str) -> Optional[KafkaTopicMetadata]:
        """Retrieve topic metadata by name."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT * FROM topic_metadata WHERE topic_name = ?
            """, (topic_name,))
            row = cursor.fetchone()
            
            if row:
                return KafkaTopicMetadata(
                    id=row['id'],
                    topic_name=row['topic_name'],
                    partitions=row['partitions'],
                    replication_factor=row['replication_factor'],
                    status=TopicStatus(row['status']),
                    description=row['description'],
                    created_at=datetime.fromisoformat(row['created_at']),
                    updated_at=datetime.fromisoformat(row['updated_at'])
                )
            return None
    
    def get_all_topics(self) -> List[KafkaTopicMetadata]:
        """Retrieve all topic metadata."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("SELECT * FROM topic_metadata ORDER BY topic_name")
            rows = cursor.fetchall()
            
            topics = []
            for row in rows:
                topics.append(KafkaTopicMetadata(
                    id=row['id'],
                    topic_name=row['topic_name'],
                    partitions=row['partitions'],
                    replication_factor=row['replication_factor'],
                    status=TopicStatus(row['status']),
                    description=row['description'],
                    created_at=datetime.fromisoformat(row['created_at']),
                    updated_at=datetime.fromisoformat(row['updated_at'])
                ))
            return topics
    
    # Offset Operations
    def upsert_offset(self, offset: KafkaOffset) -> int:
        """Insert or update offset information."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                INSERT OR REPLACE INTO kafka_offsets 
                (topic_name, partition, consumer_group, current_offset, high_water_mark, lag, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (offset.topic_name, offset.partition, offset.consumer_group,
                  offset.current_offset, offset.high_water_mark, offset.lag, offset.last_updated))
            conn.commit()
            offset.id = cursor.lastrowid
            return cursor.lastrowid
    
    def get_offsets_by_topic(self, topic_name: str) -> List[KafkaOffset]:
        """Get all offsets for a specific topic."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT * FROM kafka_offsets 
                WHERE topic_name = ? 
                ORDER BY partition, consumer_group
            """, (topic_name,))
            rows = cursor.fetchall()
            
            offsets = []
            for row in rows:
                offsets.append(KafkaOffset(
                    id=row['id'],
                    topic_name=row['topic_name'],
                    partition=row['partition'],
                    consumer_group=row['consumer_group'],
                    current_offset=row['current_offset'],
                    high_water_mark=row['high_water_mark'],
                    lag=row['lag'],
                    last_updated=datetime.fromisoformat(row['last_updated'])
                ))
            return offsets
    
    def get_offsets_by_consumer_group(self, consumer_group: str) -> List[KafkaOffset]:
        """Get all offsets for a specific consumer group."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT * FROM kafka_offsets 
                WHERE consumer_group = ? 
                ORDER BY topic_name, partition
            """, (consumer_group,))
            rows = cursor.fetchall()
            
            offsets = []
            for row in rows:
                offsets.append(KafkaOffset(
                    id=row['id'],
                    topic_name=row['topic_name'],
                    partition=row['partition'],
                    consumer_group=row['consumer_group'],
                    current_offset=row['current_offset'],
                    high_water_mark=row['high_water_mark'],
                    lag=row['lag'],
                    last_updated=datetime.fromisoformat(row['last_updated'])
                ))
            return offsets
    
    def get_consumer_lag_summary(self) -> List[Dict]:
        """Get consumer lag summary across all topics and groups."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT 
                    consumer_group,
                    topic_name,
                    SUM(lag) as total_lag,
                    COUNT(partition) as partition_count,
                    AVG(lag) as avg_lag,
                    MAX(lag) as max_lag,
                    MAX(last_updated) as last_updated
                FROM kafka_offsets 
                GROUP BY consumer_group, topic_name
                ORDER BY total_lag DESC
            """)
            rows = cursor.fetchall()
            
            return [dict(row) for row in rows]
    
    # Consumer Group Operations
    def upsert_consumer_group(self, group: ConsumerGroupInfo) -> int:
        """Insert or update consumer group information."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                INSERT OR REPLACE INTO consumer_groups 
                (group_id, state, protocol, protocol_type, members_count, coordinator, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (group.group_id, group.state, group.protocol, group.protocol_type,
                  group.members_count, group.coordinator, group.created_at))
            conn.commit()
            group.id = cursor.lastrowid
            return cursor.lastrowid
    
    def get_consumer_groups(self) -> List[ConsumerGroupInfo]:
        """Get all consumer groups."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("SELECT * FROM consumer_groups ORDER BY group_id")
            rows = cursor.fetchall()
            
            groups = []
            for row in rows:
                groups.append(ConsumerGroupInfo(
                    id=row['id'],
                    group_id=row['group_id'],
                    state=row['state'],
                    protocol=row['protocol'],
                    protocol_type=row['protocol_type'],
                    members_count=row['members_count'],
                    coordinator=row['coordinator'],
                    created_at=datetime.fromisoformat(row['created_at'])
                ))
            return groups

# Example usage
def main():
    # Initialize the database
    db = KafkaMetadataDatabase()
    
    # Create sample topic metadata
    topics = [
        KafkaTopicMetadata("user-events", 12, 3, TopicStatus.ACTIVE, "User activity events"),
        KafkaTopicMetadata("order-processing", 6, 3, TopicStatus.ACTIVE, "Order processing pipeline"),
        KafkaTopicMetadata("notifications", 3, 2, TopicStatus.ACTIVE, "Push notifications"),
        KafkaTopicMetadata("legacy-logs", 1, 1, TopicStatus.DEPRECATED, "Old logging system")
    ]
    
    # Insert topic metadata
    print("Inserting topic metadata...")
    for topic in topics:
        topic_id = db.insert_topic_metadata(topic)
        print(f"Inserted topic '{topic.topic_name}' with ID: {topic_id}")
    
    # Create sample consumer groups
    consumer_groups = [
        ConsumerGroupInfo("analytics-service", "Stable", "range", "consumer", 3, "broker-1:9092"),
        ConsumerGroupInfo("notification-sender", "Stable", "roundrobin", "consumer", 2, "broker-2:9092"),
        ConsumerGroupInfo("order-processor", "Stable", "range", "consumer", 5, "broker-1:9092")
    ]
    
    # Insert consumer groups
    print("\nInserting consumer groups...")
    for group in consumer_groups:
        group_id = db.upsert_consumer_group(group)
        print(f"Inserted consumer group '{group.group_id}' with ID: {group_id}")
    
    # Create sample offset data
    offsets = [
        KafkaOffset("user-events", 0, "analytics-service", 12500, 12800, 300),
        KafkaOffset("user-events", 1, "analytics-service", 11200, 11400, 200),
        KafkaOffset("user-events", 2, "analytics-service", 13100, 13100, 0),
        KafkaOffset("order-processing", 0, "order-processor", 5500, 5600, 100),
        KafkaOffset("order-processing", 1, "order-processor", 5200, 5250, 50),
        KafkaOffset("notifications", 0, "notification-sender", 8900, 9000, 100),
    ]
    
    # Insert offset data
    print("\nInserting offset data...")
    for offset in offsets:
        offset_id = db.upsert_offset(offset)
        print(f"Inserted offset for {offset.topic_name}:{offset.partition} "
              f"consumer group '{offset.consumer_group}' - Lag: {offset.lag}")
    
    print("\n" + "="*70)
    
    # Display all topics
    print("All Kafka Topics:")
    all_topics = db.get_all_topics()
    for topic in all_topics:
        print(f"Topic: {topic.topic_name} | Partitions: {topic.partitions} | "
              f"Status: {topic.status.value} | Description: {topic.description}")
    
    print("\n" + "="*70)
    
    # Display consumer lag summary
    print("Consumer Lag Summary:")
    lag_summary = db.get_consumer_lag_summary()
    for summary in lag_summary:
        print(f"Group: {summary['consumer_group']} | Topic: {summary['topic_name']} | "
              f"Total Lag: {summary['total_lag']} | Avg Lag: {summary['avg_lag']:.1f}")
    
    print("\n" + "="*70)
    
    # Display offsets for a specific topic
    print("Offsets for 'user-events' topic:")
    user_events_offsets = db.get_offsets_by_topic("user-events")
    for offset in user_events_offsets:
        print(f"Partition {offset.partition} | Consumer: {offset.consumer_group} | "
              f"Current: {offset.current_offset} | HWM: {offset.high_water_mark} | Lag: {offset.lag}")
    
    print("\n" + "="*70)
    
    # Display all consumer groups
    print("Consumer Groups:")
    groups = db.get_consumer_groups()
    for group in groups:
        print(f"Group: {group.group_id} | State: {group.state} | "
              f"Members: {group.members_count} | Coordinator: {group.coordinator}")

if __name__ == "__main__":
    main()
