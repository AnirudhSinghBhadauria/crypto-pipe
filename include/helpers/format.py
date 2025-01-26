from datetime import datetime, timedelta

def format_timestamps(timestamp: int):
     timestamp_s = timestamp / 1000
     local_time = datetime.fromtimestamp(timestamp_s)
     ist_time = local_time + timedelta(hours=5, minutes=30)

     return (ist_time.strftime('%Y-%m-%d %H:%M:%S'))