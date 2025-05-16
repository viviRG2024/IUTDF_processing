import pandas as pd
from datetime import datetime
import pytz

class TimeConverter:
    def __init__(self):
        pass
    
    def local_to_utc(self, local_time, timezone):
        """
        Convert local time to UTC and check for DST
        
        Parameters:
        -----------
        local_time : str or datetime
            Local time, format: "YYYY-MM-DD HH:MM" or datetime object
        timezone : str
            Timezone name, e.g. "Europe/London"
            
        Returns:
        --------
        dict
            Dictionary containing conversion results and DST information
        """
        # Ensure input is a datetime object
        if isinstance(local_time, str):
            local_time = pd.to_datetime(local_time)
            
        # Get timezone object
        tz = pytz.timezone(timezone)
        
        # Check if it's DST
        local_time_aware = tz.localize(local_time, is_dst=None)
        is_dst = local_time_aware.dst() != pd.Timedelta(0)
        
        # Convert to UTC
        utc_time = local_time_aware.astimezone(pytz.UTC)
        
        # Check if it's conversion day
        day_start = local_time.replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = local_time.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        day_hours = pd.date_range(
            start=tz.localize(day_start, is_dst=None),
            end=tz.localize(day_end, is_dst=None),
            freq='h'
        )
        
        dst_transition = None
        if len(day_hours) != 24:
            if len(day_hours) == 23:
                dst_transition = "DST starts (Spring, losing one hour)"
            elif len(day_hours) == 25:
                dst_transition = "DST ends (Fall, gaining one hour)"
        
        return {
            'input_local_time': local_time,
            'timezone': timezone,
            'utc_time': utc_time.replace(tzinfo=None),
            'is_dst': is_dst,
            'dst_transition': dst_transition
        }
    
    def utc_to_local(self, utc_time, timezone):
        """
        Convert UTC time to local time and check for DST
        
        Parameters:
        -----------
        utc_time : str or datetime
            UTC time, format: "YYYY-MM-DD HH:MM" or datetime object
        timezone : str
            Target timezone name, e.g. "Europe/London"
            
        Returns:
        --------
        dict
            Dictionary containing conversion results and DST information
        """
        # Ensure input is a datetime object
        if isinstance(utc_time, str):
            utc_time = pd.to_datetime(utc_time)
            
        # Add UTC timezone information
        utc_time_aware = pytz.UTC.localize(utc_time)
        
        # Get target timezone object
        tz = pytz.timezone(timezone)
        
        # Convert to local time
        local_time = utc_time_aware.astimezone(tz)
        
        # Check if it's DST
        is_dst = local_time.dst() != pd.Timedelta(0)
        
        # Check if it's conversion day
        utc_day_start = utc_time.replace(hour=0, minute=0, second=0, microsecond=0)
        utc_day_end = utc_time.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        local_hours = pd.date_range(
            start=pytz.UTC.localize(utc_day_start).astimezone(tz),
            end=pytz.UTC.localize(utc_day_end).astimezone(tz),
            freq='h'
        )
        
        dst_transition = None
        if len(local_hours) != 24:
            if len(local_hours) == 23:
                dst_transition = "DST starts (Spring, losing one hour)"
            elif len(local_hours) == 25:
                dst_transition = "DST ends (Fall, gaining one hour)"
        
        return {
            'input_utc_time': utc_time,
            'timezone': timezone,
            'local_time': local_time.replace(tzinfo=None),
            'is_dst': is_dst,
            'dst_transition': dst_transition
        }

def main():
    # Usage examples
    converter = TimeConverter()
    
    # Test local time to UTC conversion
    local_test = "2024-03-31 02:30"  # European DST start date
    result_local = converter.local_to_utc(local_test, "Asia/Taipei")
    print("\n=== Local Time to UTC Example ===")
    print(f"Input Local Time: {result_local['input_local_time'].strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Timezone: {result_local['timezone']}")
    print(f"Converted UTC Time: {result_local['utc_time'].strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Is DST: {'Yes' if result_local['is_dst'] else 'No'}")
    if result_local['dst_transition']:
        print(f"Special Case: {result_local['dst_transition']}")
    
    # Test UTC to local time conversion
    utc_test = "2024-10-27 01:30"  # European DST end date
    result_utc = converter.utc_to_local(utc_test, "Asia/Taipei")
    print("\n=== UTC to Local Time Example ===")
    print(f"Input UTC Time: {result_utc['input_utc_time'].strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Timezone: {result_utc['timezone']}")
    print(f"Converted Local Time: {result_utc['local_time'].strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Is DST: {'Yes' if result_utc['is_dst'] else 'No'}")
    if result_utc['dst_transition']:
        print(f"Special Case: {result_utc['dst_transition']}")

if __name__ == "__main__":
    main() 