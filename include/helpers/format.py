from datetime import datetime, timedelta

def format_timestamps(timestamp: int):
    timestamp_s = timestamp / 1000
    local_time = datetime.fromtimestamp(timestamp_s)
    ist_time = local_time + timedelta(hours=5, minutes=30)
    
    return ist_time.strftime('%d/%m %H:%M:%S')

def format_minute_datetime(date_string: str):
    dt = datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%S.%fZ')
    ist_time = dt + timedelta(hours=5, minutes=30)
    
    return ist_time.strftime('%d/%m %H:%M:%S')