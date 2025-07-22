import sqlite3
from dataclasses import dataclass, fields
from typing import List, Optional
from datetime import datetime



@dataclass
class TopicOffset:
    topic_name: str
    partition_number: int
    earliest_offset: int
    latest_offset  : int
    created_at: Optional[datetime] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now().replace(minute=(datetime.now().minute // 10)*10, second=0, microsecond=0)

            

for t in range(10):
  topic_name = 'topic-'+str(t)
  for p in range(24):
    partition_number = p
    earliest_offset  = 0 
    latest_offset    = t
    print(TopicOffset(topic_name,partition_number,earliest_offset,latest_offset))

