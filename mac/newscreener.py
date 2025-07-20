from datetime import datetime
import pytz
import pandas_market_calendars as mcal

def is_market_open_now():
    # Use the NYSE calendar
    nyse = mcal.get_calendar('NYSE')
    
    # Get current time in Eastern Time Zone
    eastern = pytz.timezone('US/Eastern')
    now_eastern = datetime.now(eastern)

    # Get trading schedule for today
    today_schedule = nyse.schedule(start_date=now_eastern.date(), end_date=now_eastern.date())

    # Check if market is open now
    if not today_schedule.empty:
        market_open = today_schedule.iloc[0]['market_open']
        market_close = today_schedule.iloc[0]['market_close']
        return market_open <= now_eastern <= market_close
    else:
        # Market is closed for the entire day (e.g., holiday)
        return False

print("Market open:", is_market_open_now())
