"""
Schema Design Optimizations

Dual Timestamp Storage: Store both Unix timestamps (for fast sorting) and ISO strings (for human readability)
Optimized Indexes: Create composite indexes on (timestamp, metric_name) for fast range queries
Pre-aggregated Tables: Store hourly/daily aggregates for faster analytical queries
JSON Tags: Use JSON strings for flexible metadata storage

Performance Configurations
sqlPRAGMA journal_mode = WAL;     -- Write-Ahead Logging for concurrency
PRAGMA synchronous = NORMAL;   -- Balanced durability/performance  
PRAGMA cache_size = -64000;    -- 64MB cache
PRAGMA mmap_size = 268435456;  -- 256MB memory mapping
Key Features of the Implementation

Batch Inserts: Efficient bulk data insertion
Time Range Queries: Fast retrieval by timestamp ranges
Aggregation Functions: Built-in support for mean, sum, min, max, etc.
Data Retention: Automatic cleanup of old data
Pandas Integration: Easy data analysis with DataFrames

Usage Examples
bash# Run the demo
python sqlite_timeseries.py

# The script will:
# 1. Create optimized tables and indexes
# 2. Generate sample data (CPU, memory, disk, network metrics)
# 3. Demonstrate various query patterns
# 4. Show performance metrics
When SQLite Works Well for Time-Series
✅ Good for:

Small to medium datasets (< 1TB)
Single-writer, multiple-reader scenarios
Embedded applications
Development and prototyping
IoT devices with local storage

❌ Consider alternatives for:

High-write concurrency (>1000 writes/sec)
Distributed deployments
Very large datasets (>1TB)
Complex analytical queries across multiple metrics

Advanced Optimizations
For production use, consider:

Partitioning: Split data across multiple database files by date
Connection Pooling: Manage concurrent access efficiently
Custom Extensions: SQLite extensions for specialized time-series functions
Compression: Store older data in compressed format

The implementation provides a solid foundation that can handle many time-series use cases efficiently with proper tuning and maintenance.
"""
#!/usr/bin/env python3
"""
SQLite Time-Series Database Implementation
Complete guide with examples for using SQLite as a time-series database
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import time
import threading
from typing import List, Dict, Any, Optional, Tuple
import contextlib

class SQLiteTimeSeriesDB:
    """
    A comprehensive SQLite-based time-series database implementation
    with optimizations for time-series data storage and retrieval
    """
    
    def __init__(self, db_path: str = "timeseries.db"):
        """Initialize the time-series database"""
        self.db_path = db_path
        self.connection = None
        self._setup_database()
    
    def _setup_database(self):
        """Setup database with optimized configuration for time-series data"""
        self.connection = sqlite3.connect(
            self.db_path,
            check_same_thread=False,
            timeout=30.0
        )
        
        # Optimize SQLite for time-series workloads
        cursor = self.connection.cursor()
        
        # Performance optimizations
        cursor.execute("PRAGMA journal_mode = WAL")  # Write-Ahead Logging
        cursor.execute("PRAGMA synchronous = NORMAL")  # Balanced durability/performance
        cursor.execute("PRAGMA cache_size = -64000")  # 64MB cache
        cursor.execute("PRAGMA temp_store = MEMORY")  # Temp tables in memory
        cursor.execute("PRAGMA mmap_size = 268435456")  # 256MB mmap
        
        self._create_tables()
        self.connection.commit()
    
    def _create_tables(self):
        """Create optimized tables for time-series data"""
        cursor = self.connection.cursor()
        
        # Main time-series table with partitioning by date
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS timeseries_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp INTEGER NOT NULL,  -- Unix timestamp for fast sorting
                datetime_str TEXT NOT NULL,   -- Human-readable datetime
                metric_name TEXT NOT NULL,
                value REAL NOT NULL,
                tags TEXT,  -- JSON string for metadata
                source TEXT,
                created_at INTEGER DEFAULT (strftime('%s', 'now'))
            )
        """)
        
        # Optimized indexes for time-series queries
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_timestamp_metric 
            ON timeseries_data(timestamp, metric_name)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_metric_timestamp 
            ON timeseries_data(metric_name, timestamp)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_datetime_str 
            ON timeseries_data(datetime_str)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_source_timestamp 
            ON timeseries_data(source, timestamp)
        """)
        
        # Aggregated data table for fast queries
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS timeseries_hourly (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                hour_timestamp INTEGER NOT NULL,
                metric_name TEXT NOT NULL,
                avg_value REAL,
                min_value REAL,
                max_value REAL,
                sum_value REAL,
                count_value INTEGER,
                first_value REAL,
                last_value REAL,
                source TEXT,
                created_at INTEGER DEFAULT (strftime('%s', 'now'))
            )
        """)
        
        cursor.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_hourly_unique 
            ON timeseries_hourly(hour_timestamp, metric_name, source)
        """)
        
        # Metadata table for metric information
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS metric_metadata (
                metric_name TEXT PRIMARY KEY,
                description TEXT,
                unit TEXT,
                data_type TEXT,
                retention_days INTEGER DEFAULT 30,
                created_at INTEGER DEFAULT (strftime('%s', 'now')),
                updated_at INTEGER DEFAULT (strftime('%s', 'now'))
            )
        """)
    
    def insert_data_point(self, timestamp: datetime, metric_name: str, 
                         value: float, tags: Dict = None, source: str = "default"):
        """Insert a single data point"""
        cursor = self.connection.cursor()
        
        unix_timestamp = int(timestamp.timestamp())
        datetime_str = timestamp.isoformat()
        tags_json = json.dumps(tags) if tags else None
        
        cursor.execute("""
            INSERT INTO timeseries_data 
            (timestamp, datetime_str, metric_name, value, tags, source)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (unix_timestamp, datetime_str, metric_name, value, tags_json, source))
        
        self.connection.commit()
    
    def insert_batch(self, data_points: List[Tuple]):
        """Insert multiple data points efficiently"""
        cursor = self.connection.cursor()
        
        processed_data = []
        for timestamp, metric_name, value, tags, source in data_points:
            unix_timestamp = int(timestamp.timestamp())
            datetime_str = timestamp.isoformat()
            tags_json = json.dumps(tags) if tags else None
            processed_data.append((
                unix_timestamp, datetime_str, metric_name, value, tags_json, source or "default"
            ))
        
        cursor.executemany("""
            INSERT INTO timeseries_data 
            (timestamp, datetime_str, metric_name, value, tags, source)
            VALUES (?, ?, ?, ?, ?, ?)
        """, processed_data)
        
        self.connection.commit()
    
    def query_range(self, metric_name: str, start_time: datetime, 
                   end_time: datetime, source: str = None) -> pd.DataFrame:
        """Query data for a specific time range"""
        query = """
            SELECT timestamp, datetime_str, metric_name, value, tags, source
            FROM timeseries_data
            WHERE metric_name = ? 
            AND timestamp BETWEEN ? AND ?
        """
        params = [metric_name, int(start_time.timestamp()), int(end_time.timestamp())]
        
        if source:
            query += " AND source = ?"
            params.append(source)
        
        query += " ORDER BY timestamp ASC"
        
        df = pd.read_sql_query(query, self.connection, params=params)
        
        if not df.empty:
            df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')
            df['tags'] = df['tags'].apply(lambda x: json.loads(x) if x else {})
        
        return df
    
    def query_latest(self, metric_name: str, limit: int = 100, 
                    source: str = None) -> pd.DataFrame:
        """Get the latest N data points for a metric"""
        query = """
            SELECT timestamp, datetime_str, metric_name, value, tags, source
            FROM timeseries_data
            WHERE metric_name = ?
        """
        params = [metric_name]
        
        if source:
            query += " AND source = ?"
            params.append(source)
        
        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)
        
        df = pd.read_sql_query(query, self.connection, params=params)
        
        if not df.empty:
            df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')
            df['tags'] = df['tags'].apply(lambda x: json.loads(x) if x else {})
            df = df.sort_values('timestamp').reset_index(drop=True)
        
        return df
    
    def aggregate_data(self, metric_name: str, start_time: datetime, 
                      end_time: datetime, interval: str = '1H', 
                      agg_func: str = 'mean') -> pd.DataFrame:
        """Aggregate data over time intervals"""
        df = self.query_range(metric_name, start_time, end_time)
        
        if df.empty:
            return df
        
        df.set_index('datetime', inplace=True)
        
        # Map aggregation functions
        agg_mapping = {
            'mean': 'mean',
            'avg': 'mean',
            'sum': 'sum',
            'min': 'min',
            'max': 'max',
            'count': 'count',
            'std': 'std',
            'first': 'first',
            'last': 'last'
        }
        
        if agg_func not in agg_mapping:
            raise ValueError(f"Unsupported aggregation function: {agg_func}")
        
        # Resample and aggregate
        result = df.groupby('metric_name').resample(interval)['value'].agg(agg_mapping[agg_func])
        result = result.reset_index()
        result.rename(columns={'value': f'{agg_func}_value'}, inplace=True)
        
        return result
    
    def create_hourly_aggregates(self):
        """Create hourly aggregates for faster queries"""
        cursor = self.connection.cursor()
        
        # Find the latest hourly aggregate timestamp
        cursor.execute("""
            SELECT MAX(hour_timestamp) FROM timeseries_hourly
        """)
        
        last_hour = cursor.fetchone()[0]
        start_timestamp = last_hour if last_hour else 0
        
        # Aggregate data by hour
        cursor.execute("""
            INSERT OR REPLACE INTO timeseries_hourly 
            (hour_timestamp, metric_name, avg_value, min_value, max_value, 
             sum_value, count_value, first_value, last_value, source)
            SELECT 
                (timestamp / 3600) * 3600 as hour_timestamp,
                metric_name,
                AVG(value) as avg_value,
                MIN(value) as min_value,
                MAX(value) as max_value,
                SUM(value) as sum_value,
                COUNT(value) as count_value,
                FIRST_VALUE(value) OVER (
                    PARTITION BY (timestamp / 3600) * 3600, metric_name, source 
                    ORDER BY timestamp
                ) as first_value,
                LAST_VALUE(value) OVER (
                    PARTITION BY (timestamp / 3600) * 3600, metric_name, source 
                    ORDER BY timestamp 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) as last_value,
                source
            FROM timeseries_data
            WHERE timestamp > ?
            GROUP BY (timestamp / 3600) * 3600, metric_name, source
        """, (start_timestamp,))
        
        self.connection.commit()
    
    def cleanup_old_data(self, retention_days: int = 30):
        """Remove old data based on retention policy"""
        cutoff_timestamp = int((datetime.now() - timedelta(days=retention_days)).timestamp())
        
        cursor = self.connection.cursor()
        
        # Delete old raw data
        cursor.execute("""
            DELETE FROM timeseries_data WHERE timestamp < ?
        """, (cutoff_timestamp,))
        
        # Delete old hourly aggregates
        cursor.execute("""
            DELETE FROM timeseries_hourly WHERE hour_timestamp < ?
        """, (cutoff_timestamp,))
        
        self.connection.commit()
        
        # Vacuum to reclaim space
        cursor.execute("VACUUM")
    
    def get_metrics_list(self) -> List[str]:
        """Get list of all available metrics"""
        cursor = self.connection.cursor()
        cursor.execute("SELECT DISTINCT metric_name FROM timeseries_data ORDER BY metric_name")
        return [row[0] for row in cursor.fetchall()]
    
    def get_database_stats(self) -> Dict:
        """Get database statistics"""
        cursor = self.connection.cursor()
        
        stats = {}
        
        # Total records
        cursor.execute("SELECT COUNT(*) FROM timeseries_data")
        stats['total_records'] = cursor.fetchone()[0]
        
        # Date range
        cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM timeseries_data")
        min_ts, max_ts = cursor.fetchone()
        if min_ts and max_ts:
            stats['date_range'] = {
                'start': datetime.fromtimestamp(min_ts).isoformat(),
                'end': datetime.fromtimestamp(max_ts).isoformat()
            }
        
        # Metrics count
        cursor.execute("SELECT COUNT(DISTINCT metric_name) FROM timeseries_data")
        stats['unique_metrics'] = cursor.fetchone()[0]
        
        # Database size
        cursor.execute("SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size()")
        stats['database_size_bytes'] = cursor.fetchone()[0]
        
        return stats
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()

# Example usage and utilities
def generate_sample_data(db: SQLiteTimeSeriesDB, num_points: int = 10000):
    """Generate sample time-series data for testing"""
    print(f"Generating {num_points} sample data points...")
    
    start_time = datetime.now() - timedelta(days=7)
    data_points = []
    
    metrics = ['cpu_usage', 'memory_usage', 'disk_io', 'network_traffic']
    sources = ['server1', 'server2', 'server3']
    
    for i in range(num_points):
        timestamp = start_time + timedelta(seconds=i * 60)  # 1 minute intervals
        metric = np.random.choice(metrics)
        source = np.random.choice(sources)
        
        # Generate realistic values based on metric type
        if metric == 'cpu_usage':
            value = max(0, min(100, np.random.normal(45, 20)))
        elif metric == 'memory_usage':
            value = max(0, min(100, np.random.normal(60, 15)))
        elif metric == 'disk_io':
            value = max(0, np.random.exponential(50))
        else:  # network_traffic
            value = max(0, np.random.exponential(100))
        
        tags = {'environment': 'production', 'region': 'us-west-2'}
        data_points.append((timestamp, metric, value, tags, source))
        
        # Batch insert every 1000 points
        if len(data_points) >= 1000:
            db.insert_batch(data_points)
            data_points = []
            print(f"Inserted {i+1} points...")
    
    # Insert remaining points
    if data_points:
        db.insert_batch(data_points)
    
    print("Sample data generation complete!")

def demonstrate_queries(db: SQLiteTimeSeriesDB):
    """Demonstrate various query patterns"""
    print("\n" + "="*50)
    print("QUERY DEMONSTRATIONS")
    print("="*50)
    
    # Get available metrics
    metrics = db.get_metrics_list()
    print(f"Available metrics: {metrics}")
    
    if not metrics:
        print("No data available. Generate sample data first.")
        return
    
    # Query latest data
    print(f"\nLatest 10 data points for {metrics[0]}:")
    latest_data = db.query_latest(metrics[0], limit=10)
    print(latest_data[['datetime_str', 'value', 'source']].to_string(index=False))
    
    # Query time range
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=24)
    
    print(f"\nData for {metrics[0]} in last 24 hours:")
    range_data = db.query_range(metrics[0], start_time, end_time)
    print(f"Found {len(range_data)} data points")
    
    if len(range_data) > 0:
        print(f"Value range: {range_data['value'].min():.2f} - {range_data['value'].max():.2f}")
        print(f"Average: {range_data['value'].mean():.2f}")
    
    # Aggregated query
    if len(range_data) > 0:
        print(f"\nHourly averages for {metrics[0]}:")
        hourly_avg = db.aggregate_data(metrics[0], start_time, end_time, '1H', 'mean')
        if not hourly_avg.empty:
            print(hourly_avg[['datetime', 'mean_value']].head().to_string(index=False))
    
    # Database stats
    print("\nDatabase Statistics:")
    stats = db.get_database_stats()
    for key, value in stats.items():
        print(f"  {key}: {value}")

def main():
    """Main demonstration function"""
    print("SQLite Time-Series Database Demo")
    print("="*40)
    
    # Initialize database
    db = SQLiteTimeSeriesDB("demo_timeseries.db")
    
    try:
        # Check if we have data
        stats = db.get_database_stats()
        
        if stats['total_records'] == 0:
            # Generate sample data
            generate_sample_data(db, 5000)
            
            # Create hourly aggregates
            print("Creating hourly aggregates...")
            db.create_hourly_aggregates()
        
        # Demonstrate queries
        demonstrate_queries(db)
        
        # Performance test
        print(f"\n{'='*50}")
        print("PERFORMANCE TEST")
        print("="*50)
        
        metric_name = db.get_metrics_list()[0]
        end_time = datetime.now()
        start_time = end_time - timedelta(days=1)
        
        # Time the query
        start = time.time()
        result = db.query_range(metric_name, start_time, end_time)
        query_time = time.time() - start
        
        print(f"Query returned {len(result)} records in {query_time:.3f} seconds")
        print(f"Query rate: {len(result)/query_time:.0f} records/second")
        
    finally:
        db.close()

if __name__ == "__main__":
    main()

# Additional optimization tips:
"""
SQLITE TIME-SERIES OPTIMIZATION TIPS:

1. Schema Design:
   - Use INTEGER timestamps for sorting performance
   - Include both unix timestamp and ISO string for flexibility
   - Create composite indexes on (timestamp, metric_name)
   - Consider partitioning large datasets by date

2. Write Optimization:
   - Use batch inserts with executemany()
   - Enable WAL mode for concurrent reads during writes
   - Use transactions for bulk operations

3. Read Optimization:
   - Create pre-aggregated tables for common queries
   - Use appropriate indexes for your query patterns
   - Consider LIMIT clauses for large result sets

4. Maintenance:
   - Implement data retention policies
   - Regular VACUUM operations to reclaim space
   - Monitor database size and performance

5. Advanced Features:
   - Use virtual tables for real-time calculations
   - Implement custom functions for complex aggregations
   - Consider SQLite extensions like time-series extensions
"""

