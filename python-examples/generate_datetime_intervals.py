from datetime import datetime, timedelta
import pytz

def generate_datetime_intervals(start_time=None, duration_hours=2, interval_minutes=10):
    """
    Generate datetime stamps at specified minute intervals
    
    Args:
        start_time: Starting datetime (default: current time)
        duration_hours: How many hours to generate timestamps for
        interval_minutes: Interval between timestamps in minutes
    
    Returns:
        List of datetime objects
    """
    if start_time is None:
        start_time = datetime.now()
    
    timestamps = []
    current_time = start_time
    end_time = start_time + timedelta(hours=duration_hours)
    
    while current_time <= end_time:
        timestamps.append(current_time)
        current_time += timedelta(minutes=interval_minutes)
    
    return timestamps

def generate_timezone_intervals(start_time=None, timezone='UTC', duration_hours=2, interval_minutes=10):
    """
    Generate datetime stamps with timezone awareness
    """
    tz = pytz.timezone(timezone)
    
    if start_time is None:
        start_time = datetime.now(tz)
    elif start_time.tzinfo is None:
        start_time = tz.localize(start_time)
    
    timestamps = []
    current_time = start_time
    end_time = start_time + timedelta(hours=duration_hours)
    
    while current_time <= end_time:
        timestamps.append(current_time)
        current_time += timedelta(minutes=interval_minutes)
    
    return timestamps

def generate_business_hours_intervals(start_date=None, business_start_hour=9, 
                                    business_end_hour=17, interval_minutes=10, num_days=5):
    """
    Generate timestamps only during business hours (9 AM - 5 PM) for weekdays
    """
    if start_date is None:
        start_date = datetime.now().date()
    
    timestamps = []
    current_date = start_date
    days_processed = 0
    
    while days_processed < num_days:
        # Skip weekends (Monday=0, Sunday=6)
        if current_date.weekday() < 5:  # Monday to Friday
            # Start at business hours
            start_time = datetime.combine(current_date, datetime.min.time().replace(hour=business_start_hour))
            end_time = datetime.combine(current_date, datetime.min.time().replace(hour=business_end_hour))
            
            current_time = start_time
            while current_time <= end_time:
                timestamps.append(current_time)
                current_time += timedelta(minutes=interval_minutes)
            
            days_processed += 1
        
        current_date += timedelta(days=1)
    
    return timestamps

def generate_custom_intervals(start_time, intervals):
    """
    Generate timestamps using custom interval list
    
    Args:
        start_time: Starting datetime
        intervals: List of minute intervals [10, 15, 5, 20, ...]
    """
    timestamps = [start_time]
    current_time = start_time
    
    for interval in intervals:
        current_time += timedelta(minutes=interval)
        timestamps.append(current_time)
    
    return timestamps

def format_timestamps(timestamps, format_string='%Y-%m-%d %H:%M:%S'):
    """
    Format timestamps as strings
    """
    return [ts.strftime(format_string) for ts in timestamps]

# Example usage and demonstrations
if __name__ == "__main__":
    print("=== DateTime Interval Generator Examples ===\n")
    
    # Example 1: Basic 10-minute intervals
    print("1. Basic 10-minute intervals (2 hours):")
    start = datetime(2024, 7, 20, 14, 30, 0)  # 2:30 PM
    intervals = generate_datetime_intervals(start, duration_hours=2, interval_minutes=10)
    
    for ts in intervals:
        print(f"   {ts.strftime('%Y-%m-%d %H:%M:%S')}")
    
    print(f"\n   Generated {len(intervals)} timestamps\n")
    
    # Example 2: Different intervals (5, 15, 30 minutes)
    print("2. Different interval examples:")
    
    for minutes in [5, 15, 30]:
        print(f"\n   {minutes}-minute intervals (1 hour):")
        intervals = generate_datetime_intervals(start, duration_hours=1, interval_minutes=minutes)
        formatted = format_timestamps(intervals, '%H:%M')
        print(f"   {', '.join(formatted)}")
    
    # Example 3: Timezone-aware timestamps
    print("\n3. Timezone-aware timestamps (10-min intervals, 1 hour):")
    
    timezones = ['UTC', 'US/Eastern', 'US/Pacific', 'Europe/London']
    for tz in timezones:
        print(f"\n   {tz}:")
        tz_intervals = generate_timezone_intervals(start, timezone=tz, duration_hours=1, interval_minutes=10)
        for ts in tz_intervals[:4]:  # Show first 4
            print(f"     {ts.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    # Example 4: Business hours only
    print("\n4. Business hours intervals (10-min, 2 days):")
    business_intervals = generate_business_hours_intervals(
        start_date=datetime(2024, 7, 22).date(),  # Monday
        interval_minutes=10, 
        num_days=2
    )
    
    current_day = None
    for ts in business_intervals:
        if current_day != ts.date():
            current_day = ts.date()
            print(f"\n   {current_day.strftime('%A, %Y-%m-%d')}:")
        print(f"     {ts.strftime('%H:%M')}")
    
    # Example 5: Custom intervals
    print("\n5. Custom interval pattern:")
    custom_intervals = [10, 15, 5, 20, 10, 30]
    custom_stamps = generate_custom_intervals(start, custom_intervals)
    
    print(f"   Intervals: {custom_intervals} minutes")
    for i, ts in enumerate(custom_stamps):
        if i == 0:
            print(f"   Start: {ts.strftime('%H:%M:%S')}")
        else:
            diff = (ts - custom_stamps[i-1]).total_seconds() / 60
            print(f"   +{int(diff)}min: {ts.strftime('%H:%M:%S')}")
    
    # Example 6: Generate data with timestamps
    print("\n6. Sample data with timestamps:")
    data_intervals = generate_datetime_intervals(
        datetime.now().replace(second=0, microsecond=0), 
        duration_hours=1, 
        interval_minutes=10
    )
    
    import random
    sample_data = []
    for ts in data_intervals:
        sample_data.append({
            'timestamp': ts.strftime('%Y-%m-%d %H:%M:%S'),
            'value': round(random.uniform(10.0, 100.0), 2),
            'status': random.choice(['active', 'idle', 'processing'])
        })
    
    print("   Sample sensor data:")
    for data in sample_data[:5]:  # Show first 5
        print(f"     {data['timestamp']} | Value: {data['value']} | Status: {data['status']}")

# Utility functions for specific use cases
def generate_log_timestamps(start_time, num_entries, base_interval=10, jitter_seconds=30):
    """
    Generate timestamps for log entries with some randomness/jitter
    """
    timestamps = []
    current_time = start_time
    
    for _ in range(num_entries):
        timestamps.append(current_time)
        # Add base interval plus some random jitter
        jitter = timedelta(seconds=random.randint(-jitter_seconds, jitter_seconds))
        current_time += timedelta(minutes=base_interval) + jitter
    
    return timestamps

def generate_monitoring_schedule(start_time, end_time, check_intervals):
    """
    Generate monitoring check schedule with different intervals for different times
    
    Args:
        start_time: Start datetime
        end_time: End datetime  
        check_intervals: Dict with hour ranges and intervals
                        e.g., {(9, 17): 5, (17, 9): 15}  # 5min during business, 15min after
    """
    timestamps = []
    current_time = start_time
    
    while current_time <= end_time:
        current_hour = current_time.hour
        
        # Find appropriate interval based on current hour
        interval_minutes = 10  # default
        for (start_h, end_h), minutes in check_intervals.items():
            if start_h <= end_h:  # Same day range
                if start_h <= current_hour < end_h:
                    interval_minutes = minutes
                    break
            else:  # Overnight range
                if current_hour >= start_h or current_hour < end_h:
                    interval_minutes = minutes
                    break
        
        timestamps.append(current_time)
        current_time += timedelta(minutes=interval_minutes)
    
    return timestamps
