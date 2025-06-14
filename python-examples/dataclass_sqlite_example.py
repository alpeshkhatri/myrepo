from dataclasses import dataclass
from typing import List, Optional, Dict
from datetime import datetime
from enum import Enum
import os

from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey, UniqueConstraint, func, Enum as SQLEnum
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship
from sqlalchemy.exc import IntegrityError

# Base class for all ORM models
Base = declarative_base()

class TopicStatus(Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    DEPRECATED = "deprecated"

class ConsumerGroupState(Enum):
    STABLE = "Stable"
    DEAD = "Dead"
    EMPTY = "Empty"
    PREPARING_REBALANCE = "PreparingRebalance"
    COMPLETING_REBALANCE = "CompletingRebalance"

# SQLAlchemy ORM Models
class TopicMetadata(Base):
    __tablename__ = 'topic_metadata'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    topic_name = Column(String(255), unique=True, nullable=False, index=True)
    partitions = Column(Integer, nullable=False)
    replication_factor = Column(Integer, nullable=False)
    status = Column(SQLEnum(TopicStatus), nullable=False, default=TopicStatus.ACTIVE)
    description = Column(String(500))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    offsets = relationship("KafkaOffset", back_populates="topic", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<TopicMetadata(topic_name='{self.topic_name}', partitions={self.partitions}, status='{self.status.value}')>"

class ConsumerGroup(Base):
    __tablename__ = 'consumer_groups'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    group_id = Column(String(255), unique=True, nullable=False, index=True)
    state = Column(SQLEnum(ConsumerGroupState), nullable=False)
    protocol = Column(String(50))
    protocol_type = Column(String(50))
    members_count = Column(Integer, default=0)
    coordinator = Column(String(100))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    offsets = relationship("KafkaOffset", back_populates="consumer_group", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<ConsumerGroup(group_id='{self.group_id}', state='{self.state.value}', members={self.members_count})>"

class KafkaOffset(Base):
    __tablename__ = 'kafka_offsets'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    topic_name = Column(String(255), ForeignKey('topic_metadata.topic_name'), nullable=False)
    partition = Column(Integer, nullable=False)
    consumer_group_id = Column(String(255), ForeignKey('consumer_groups.group_id'), nullable=False)
    current_offset = Column(Integer, nullable=False)
    high_water_mark = Column(Integer, nullable=False)
    lag = Column(Integer, nullable=False)
    last_updated = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    topic = relationship("TopicMetadata", back_populates="offsets")
    consumer_group = relationship("ConsumerGroup", back_populates="offsets")
    
    # Unique constraint for topic-partition-consumer group combination
    __table_args__ = (
        UniqueConstraint('topic_name', 'partition', 'consumer_group_id', name='_topic_partition_consumer_uc'),
    )
    
    def __repr__(self):
        return f"<KafkaOffset(topic='{self.topic_name}', partition={self.partition}, group='{self.consumer_group_id}', lag={self.lag})>"

# Dataclasses for data transfer and API interactions
@dataclass
class KafkaTopicMetadataDTO:
    topic_name: str
    partitions: int
    replication_factor: int
    status: TopicStatus
    description: str
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    id: Optional[int] = None

@dataclass
class KafkaOffsetDTO:
    topic_name: str
    partition: int
    consumer_group_id: str
    current_offset: int
    high_water_mark: int
    lag: Optional[int] = None
    last_updated: Optional[datetime] = None
    id: Optional[int] = None
    
    def __post_init__(self):
        if self.lag is None:
            self.lag = max(0, self.high_water_mark - self.current_offset)

@dataclass
class ConsumerGroupDTO:
    group_id: str
    state: ConsumerGroupState
    protocol: str
    protocol_type: str
    members_count: int
    coordinator: str
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    id: Optional[int] = None

# Database Service Class
class KafkaMetadataService:
    def __init__(self, database_url: str = "sqlite:///kafka_metadata.db"):
        self.engine = create_engine(database_url, echo=False)
        Base.metadata.create_all(self.engine)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
    
    def get_session(self) -> Session:
        return self.SessionLocal()
    
    # Topic Operations
    def create_or_update_topic(self, topic_dto: KafkaTopicMetadataDTO) -> TopicMetadata:
        """Create or update topic metadata."""
        with self.get_session() as session:
            topic = session.query(TopicMetadata).filter_by(topic_name=topic_dto.topic_name).first()
            
            if topic:
                # Update existing topic
                topic.partitions = topic_dto.partitions
                topic.replication_factor = topic_dto.replication_factor
                topic.status = topic_dto.status
                topic.description = topic_dto.description
                topic.updated_at = datetime.utcnow()
            else:
                # Create new topic
                topic = TopicMetadata(
                    topic_name=topic_dto.topic_name,
                    partitions=topic_dto.partitions,
                    replication_factor=topic_dto.replication_factor,
                    status=topic_dto.status,
                    description=topic_dto.description
                )
                session.add(topic)
            
            session.commit()
            session.refresh(topic)
            return topic
    
    def get_topic_by_name(self, topic_name: str) -> Optional[TopicMetadata]:
        """Get topic by name."""
        with self.get_session() as session:
            return session.query(TopicMetadata).filter_by(topic_name=topic_name).first()
    
    def get_all_topics(self) -> List[TopicMetadata]:
        """Get all topics."""
        with self.get_session() as session:
            return session.query(TopicMetadata).order_by(TopicMetadata.topic_name).all()
    
    def get_topics_by_status(self, status: TopicStatus) -> List[TopicMetadata]:
        """Get topics by status."""
        with self.get_session() as session:
            return session.query(TopicMetadata).filter_by(status=status).all()
    
    # Consumer Group Operations
    def create_or_update_consumer_group(self, group_dto: ConsumerGroupDTO) -> ConsumerGroup:
        """Create or update consumer group."""
        with self.get_session() as session:
            group = session.query(ConsumerGroup).filter_by(group_id=group_dto.group_id).first()
            
            if group:
                # Update existing group
                group.state = group_dto.state
                group.protocol = group_dto.protocol
                group.protocol_type = group_dto.protocol_type
                group.members_count = group_dto.members_count
                group.coordinator = group_dto.coordinator
                group.updated_at = datetime.utcnow()
            else:
                # Create new group
                group = ConsumerGroup(
                    group_id=group_dto.group_id,
                    state=group_dto.state,
                    protocol=group_dto.protocol,
                    protocol_type=group_dto.protocol_type,
                    members_count=group_dto.members_count,
                    coordinator=group_dto.coordinator
                )
                session.add(group)
            
            session.commit()
            session.refresh(group)
            return group
    
    def get_all_consumer_groups(self) -> List[ConsumerGroup]:
        """Get all consumer groups."""
        with self.get_session() as session:
            return session.query(ConsumerGroup).order_by(ConsumerGroup.group_id).all()
    
    # Offset Operations
    def upsert_offset(self, offset_dto: KafkaOffsetDTO) -> KafkaOffset:
        """Insert or update offset information."""
        with self.get_session() as session:
            offset = session.query(KafkaOffset).filter_by(
                topic_name=offset_dto.topic_name,
                partition=offset_dto.partition,
                consumer_group_id=offset_dto.consumer_group_id
            ).first()
            
            if offset:
                # Update existing offset
                offset.current_offset = offset_dto.current_offset
                offset.high_water_mark = offset_dto.high_water_mark
                offset.lag = offset_dto.lag
                offset.last_updated = datetime.utcnow()
            else:
                # Create new offset
                offset = KafkaOffset(
                    topic_name=offset_dto.topic_name,
                    partition=offset_dto.partition,
                    consumer_group_id=offset_dto.consumer_group_id,
                    current_offset=offset_dto.current_offset,
                    high_water_mark=offset_dto.high_water_mark,
                    lag=offset_dto.lag
                )
                session.add(offset)
            
            session.commit()
            session.refresh(offset)
            return offset
    
    def get_offsets_by_topic(self, topic_name: str) -> List[KafkaOffset]:
        """Get all offsets for a topic."""
        with self.get_session() as session:
            return session.query(KafkaOffset).filter_by(topic_name=topic_name)\
                         .order_by(KafkaOffset.partition, KafkaOffset.consumer_group_id).all()
    
    def get_offsets_by_consumer_group(self, group_id: str) -> List[KafkaOffset]:
        """Get all offsets for a consumer group."""
        with self.get_session() as session:
            return session.query(KafkaOffset).filter_by(consumer_group_id=group_id)\
                         .order_by(KafkaOffset.topic_name, KafkaOffset.partition).all()
    
    def get_consumer_lag_summary(self) -> List[Dict]:
        """Get consumer lag summary with aggregated statistics."""
        with self.get_session() as session:
            result = session.query(
                KafkaOffset.consumer_group_id,
                KafkaOffset.topic_name,
                func.sum(KafkaOffset.lag).label('total_lag'),
                func.count(KafkaOffset.partition).label('partition_count'),
                func.avg(KafkaOffset.lag).label('avg_lag'),
                func.max(KafkaOffset.lag).label('max_lag'),
                func.max(KafkaOffset.last_updated).label('last_updated')
            ).group_by(
                KafkaOffset.consumer_group_id, 
                KafkaOffset.topic_name
            ).order_by(
                func.sum(KafkaOffset.lag).desc()
            ).all()
            
            return [
                {
                    'consumer_group': row.consumer_group_id,
                    'topic_name': row.topic_name,
                    'total_lag': row.total_lag,
                    'partition_count': row.partition_count,
                    'avg_lag': float(row.avg_lag) if row.avg_lag else 0,
                    'max_lag': row.max_lag,
                    'last_updated': row.last_updated
                }
                for row in result
            ]
    
    def get_topics_with_high_lag(self, lag_threshold: int = 1000) -> List[Dict]:
        """Get topics with consumer lag above threshold."""
        with self.get_session() as session:
            result = session.query(
                TopicMetadata.topic_name,
                TopicMetadata.status,
                func.sum(KafkaOffset.lag).label('total_lag'),
                func.max(KafkaOffset.lag).label('max_lag')
            ).join(
                KafkaOffset, TopicMetadata.topic_name == KafkaOffset.topic_name
            ).group_by(
                TopicMetadata.topic_name, TopicMetadata.status
            ).having(
                func.sum(KafkaOffset.lag) > lag_threshold
            ).order_by(
                func.sum(KafkaOffset.lag).desc()
            ).all()
            
            return [
                {
                    'topic_name': row.topic_name,
                    'status': row.status.value,
                    'total_lag': row.total_lag,
                    'max_lag': row.max_lag
                }
                for row in result
            ]

# Example usage
def main():
    # Initialize the service
    service = KafkaMetadataService()
    
    # Create sample topics
    topics_data = [
        KafkaTopicMetadataDTO("user-events", 12, 3, TopicStatus.ACTIVE, "User activity events"),
        KafkaTopicMetadataDTO("order-processing", 6, 3, TopicStatus.ACTIVE, "Order processing pipeline"),
        KafkaTopicMetadataDTO("notifications", 3, 2, TopicStatus.ACTIVE, "Push notifications"),
        KafkaTopicMetadataDTO("legacy-logs", 1, 1, TopicStatus.DEPRECATED, "Old logging system")
    ]
    
    print("Creating topics...")
    for topic_dto in topics_data:
        topic = service.create_or_update_topic(topic_dto)
        print(f"Created/Updated: {topic}")
    
    # Create consumer groups
    groups_data = [
        ConsumerGroupDTO("analytics-service", ConsumerGroupState.STABLE, "range", "consumer", 3, "broker-1:9092"),
        ConsumerGroupDTO("notification-sender", ConsumerGroupState.STABLE, "roundrobin", "consumer", 2, "broker-2:9092"),
        ConsumerGroupDTO("order-processor", ConsumerGroupState.STABLE, "range", "consumer", 5, "broker-1:9092")
    ]
    
    print("\nCreating consumer groups...")
    for group_dto in groups_data:
        group = service.create_or_update_consumer_group(group_dto)
        print(f"Created/Updated: {group}")
    
    # Create offset data
    offsets_data = [
        KafkaOffsetDTO("user-events", 0, "analytics-service", 12500, 12800),
        KafkaOffsetDTO("user-events", 1, "analytics-service", 11200, 11400),
        KafkaOffsetDTO("user-events", 2, "analytics-service", 13100, 13100),
        KafkaOffsetDTO("order-processing", 0, "order-processor", 5500, 5600),
        KafkaOffsetDTO("order-processing", 1, "order-processor", 5200, 5250),
        KafkaOffsetDTO("notifications", 0, "notification-sender", 8900, 9000),
    ]
    
    print("\nUpserting offset data...")
    for offset_dto in offsets_data:
        offset = service.upsert_offset(offset_dto)
        print(f"Upserted: {offset}")
    
    print("\n" + "="*80)
    
    # Query examples
    print("All Topics:")
    topics = service.get_all_topics()
    for topic in topics:
        print(f"  {topic}")
    
    print(f"\nActive Topics:")
    active_topics = service.get_topics_by_status(TopicStatus.ACTIVE)
    for topic in active_topics:
        print(f"  {topic}")
    
    print(f"\nConsumer Lag Summary:")
    lag_summary = service.get_consumer_lag_summary()
    for summary in lag_summary:
        print(f"  Group: {summary['consumer_group']} | Topic: {summary['topic_name']} | "
              f"Total Lag: {summary['total_lag']} | Avg: {summary['avg_lag']:.1f}")
    
    print(f"\nTopics with High Lag (>100):")
    high_lag_topics = service.get_topics_with_high_lag(100)
    for topic_info in high_lag_topics:
        print(f"  Topic: {topic_info['topic_name']} | Status: {topic_info['status']} | "
              f"Total Lag: {topic_info['total_lag']}")
    
    print(f"\nOffsets for 'user-events':")
    user_events_offsets = service.get_offsets_by_topic("user-events")
    for offset in user_events_offsets:
        print(f"  {offset}")

if __name__ == "__main__":
    main()
