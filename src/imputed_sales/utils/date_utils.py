"""
Date and time utilities for the Imputed Sales Analytics Platform.

This module provides functions for working with dates, times, and timestamps
in a PySpark environment.
"""

import logging
from datetime import datetime, date, timedelta
from typing import Union, List, Tuple, Optional, Dict, Any
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Column
from pyspark.sql.types import DateType, TimestampType

logger = logging.getLogger(__name__)

def get_current_date(spark) -> date:
    """
    Get the current date as a Python date object.
    
    Args:
        spark: SparkSession
        
    Returns:
        Current date
    """
    return datetime.utcnow().date()


def get_current_timestamp(spark) -> datetime:
    """
    Get the current timestamp as a Python datetime object.
    
    Args:
        spark: SparkSession
        
    Returns:
        Current timestamp
    """
    return datetime.utcnow()


def to_date_col(
    col: Union[str, Column], 
    date_format: str = 'yyyy-MM-dd'
) -> Column:
    """
    Convert a string column to a date column.
    
    Args:
        col: Column name or Column object
        date_format: Source date format string
        
    Returns:
        Date column
    """
    if isinstance(col, str):
        col = F.col(col)
    return F.to_date(col, date_format)


def to_timestamp_col(
    col: Union[str, Column], 
    timestamp_format: str = 'yyyy-MM-dd HH:mm:ss'
) -> Column:
    """
    Convert a string column to a timestamp column.
    
    Args:
        col: Column name or Column object
        timestamp_format: Source timestamp format string
        
    Returns:
        Timestamp column
    """
    if isinstance(col, str):
        col = F.col(col)
    return F.to_timestamp(col, timestamp_format)


def format_date_col(
    col: Union[str, Column], 
    output_format: str = 'yyyy-MM-dd'
) -> Column:
    """
    Format a date/timestamp column as a string.
    
    Args:
        col: Column name or Column object
        output_format: Target date format string
        
    Returns:
        Formatted string column
    """
    if isinstance(col, str):
        col = F.col(col)
    return F.date_format(col, output_format)


def add_days(
    date_col: Union[str, Column], 
    days: int
) -> Column:
    """
    Add days to a date column.
    
    Args:
        date_col: Date column or column name
        days: Number of days to add (can be negative)
        
    Returns:
        New date column
    """
    if isinstance(date_col, str):
        date_col = F.col(date_col)
    return F.date_add(date_col, days)


def date_diff(
    end_date: Union[str, Column], 
    start_date: Union[str, Column], 
    unit: str = 'day'
) -> Column:
    """
    Calculate the difference between two dates.
    
    Args:
        end_date: End date column or column name
        start_date: Start date column or column name
        unit: Unit of the difference ('day', 'month', 'year')
        
    Returns:
        Difference as an integer column
    """
    if isinstance(end_date, str):
        end_date = F.col(end_date)
    if isinstance(start_date, str):
        start_date = F.col(start_date)
    
    if unit.lower() == 'day':
        return F.datediff(end_date, start_date)
    elif unit.lower() == 'month':
        return F.months_between(end_date, start_date, roundOff=False)
    elif unit.lower() == 'year':
        return F.months_between(end_date, start_date, roundOff=False) / 12
    else:
        raise ValueError(f"Unsupported unit: {unit}")


def get_date_range(
    start_date: Union[str, date],
    end_date: Union[str, date],
    freq: str = 'day',
    date_format: str = '%Y-%m-%d'
) -> List[date]:
    """
    Generate a sequence of dates between start_date and end_date.
    
    Args:
        start_date: Start date (string or date object)
        end_date: End date (string or date object)
        freq: Frequency ('day', 'week', 'month', 'quarter', 'year')
        date_format: Format string if dates are provided as strings
        
    Returns:
        List of date objects
    """
    # Convert string dates to date objects if needed
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, date_format).date()
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, date_format).date()
    
    if start_date > end_date:
        raise ValueError("start_date must be before or equal to end_date")
    
    delta_map = {
        'day': timedelta(days=1),
        'week': timedelta(weeks=1),
        'month': None,  # Handled separately
        'quarter': None,  # Handled separately
        'year': None  # Handled separately
    }
    
    if freq not in delta_map:
        raise ValueError(f"Unsupported frequency: {freq}")
    
    dates = []
    current = start_date
    
    if freq == 'day' or freq == 'week':
        delta = delta_map[freq]
        while current <= end_date:
            dates.append(current)
            current += delta
    
    elif freq == 'month':
        while current <= end_date:
            dates.append(current)
            # Move to first day of next month
            if current.month == 12:
                current = date(current.year + 1, 1, 1)
            else:
                current = date(current.year, current.month + 1, 1)
    
    elif freq == 'quarter':
        # Adjust to start of current quarter
        quarter_start_month = ((current.month - 1) // 3) * 3 + 1
        current = date(current.year, quarter_start_month, 1)
        
        while current <= end_date:
            dates.append(current)
            # Move to first day of next quarter
            next_month = current.month + 3
            if next_month > 12:
                current = date(current.year + 1, next_month - 12, 1)
            else:
                current = date(current.year, next_month, 1)
    
    elif freq == 'year':
        # Adjust to start of year
        current = date(current.year, 1, 1)
        
        while current <= end_date:
            dates.append(current)
            # Move to first day of next year
            current = date(current.year + 1, 1, 1)
    
    return dates


def get_fiscal_quarter(date_col: Union[str, Column]) -> Column:
    """
    Get the fiscal quarter (April-March) for a date column.
    
    Args:
        date_col: Date column or column name
        
    Returns:
        String column with fiscal quarter in 'FY{end_year}Q{quarter}' format
    """
    if isinstance(date_col, str):
        date_col = F.col(date_col)
    
    month = F.month(date_col)
    year = F.year(date_col)
    
    # Fiscal year starts in April (Q1: Apr-Jun, Q2: Jul-Sep, Q3: Oct-Dec, Q4: Jan-Mar)
    fiscal_quarter = F.when(month >= 4, ((month - 4) // 3) + 1) \
                     .otherwise(((month + 8) // 3) + 1)
    
    # Fiscal year is the calendar year in which it ends
    fiscal_year = F.when(month >= 4, year + 1).otherwise(year)
    
    return F.concat(
        F.lit('FY'),
        F.substring(F.year(fiscal_year).cast('string'), 3, 2),
        F.lit('Q'),
        fiscal_quarter.cast('string')
    )


def get_week_start(date_col: Union[str, Column]) -> Column:
    """
    Get the start of the week (Monday) for a date column.
    
    Args:
        date_col: Date column or column name
        
    Returns:
        Date column with the start of the week
    """
    if isinstance(date_col, str):
        date_col = F.col(date_col)
    
    # Spark's week starts on Sunday (1) by default, so we adjust to start on Monday (2)
    return F.date_sub(
        F.next_day(date_col, 'Sunday'),
        F.when(F.dayofweek(date_col) == 1, 6).otherwise(F.dayofweek(date_col) - 2)
    )


def get_month_start(date_col: Union[str, Column]) -> Column:
    """
    Get the first day of the month for a date column.
    
    Args:
        date_col: Date column or column name
        
    Returns:
        Date column with the first day of the month
    """
    if isinstance(date_col, str):
        date_col = F.col(date_col)
    
    return F.trunc(date_col, 'month')


def get_quarter_start(date_col: Union[str, Column]) -> Column:
    """
    Get the first day of the quarter for a date column.
    
    Args:
        date_col: Date column or column name
        
    Returns:
        Date column with the first day of the quarter
    """
    if isinstance(date_col, str):
        date_col = F.col(date_col)
    
    return F.trunc(date_col, 'quarter')


def get_year_start(date_col: Union[str, Column]) -> Column:
    """
    Get the first day of the year for a date column.
    
    Args:
        date_col: Date column or column name
        
    Returns:
        Date column with the first day of the year
    """
    if isinstance(date_col, str):
        date_col = F.col(date_col)
    
    return F.trunc(date_col, 'year')


def add_months(
    date_col: Union[str, Column], 
    months: int
) -> Column:
    """
    Add months to a date column.
    
    Args:
        date_col: Date column or column name
        months: Number of months to add (can be negative)
        
    Returns:
        New date column
    """
    if isinstance(date_col, str):
        date_col = F.col(date_col)
    
    return F.add_months(date_col, months)


def get_last_day_of_month(date_col: Union[str, Column]) -> Column:
    """
    Get the last day of the month for a date column.
    
    Args:
        date_col: Date column or column name
        
    Returns:
        Date column with the last day of the month
    """
    if isinstance(date_col, str):
        date_col = F.col(date_col)
    
    return F.last_day(date_col)


def is_weekend(date_col: Union[str, Column]) -> Column:
    """
    Check if a date falls on a weekend (Saturday or Sunday).
    
    Args:
        date_col: Date column or column name
        
    Returns:
        Boolean column indicating if the date is a weekend
    """
    if isinstance(date_col, str):
        date_col = F.col(date_col)
    
    return F.dayofweek(date_col).isin([1, 7])  # 1=Sunday, 7=Saturday


def is_holiday(
    date_col: Union[str, Column],
    holidays: List[date],
    date_format: str = '%Y-%m-%d'
) -> Column:
    """
    Check if a date is in the list of holidays.
    
    Args:
        date_col: Date column or column name
        holidays: List of holiday dates (as date objects or strings)
        date_format: Format string if holidays are provided as strings
        
    Returns:
        Boolean column indicating if the date is a holiday
    """
    if isinstance(date_col, str):
        date_col = F.col(date_col)
    
    # Convert string dates to date objects if needed
    holiday_dates = []
    for holiday in holidays:
        if isinstance(holiday, str):
            holiday = datetime.strptime(holiday, date_format).date()
        holiday_dates.append(holiday)
    
    # Create a column expression that checks if the date is in the holidays list
    is_holiday = F.lit(False)
    for holiday in holiday_dates:
        is_holiday = is_holiday | (date_col == F.lit(holiday).cast('date'))
    
    return is_holiday


def get_business_days(
    start_date: Union[str, date],
    end_date: Union[str, date],
    holidays: Optional[List[Union[str, date]]] = None,
    date_format: str = '%Y-%m-%d'
) -> int:
    """
    Calculate the number of business days between two dates.
    
    Args:
        start_date: Start date (string or date object)
        end_date: End date (string or date object)
        holidays: Optional list of holiday dates (as date objects or strings)
        date_format: Format string if dates are provided as strings
        
    Returns:
        Number of business days (inclusive of start and end dates)
    """
    # Convert string dates to date objects if needed
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, date_format).date()
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, date_format).date()
    
    if start_date > end_date:
        start_date, end_date = end_date, start_date
    
    # Convert holidays to date objects if needed
    holiday_dates = set()
    if holidays:
        for holiday in holidays:
            if isinstance(holiday, str):
                holiday = datetime.strptime(holiday, date_format).date()
            holiday_dates.add(holiday)
    
    # Calculate all days between start and end (inclusive)
    delta = end_date - start_date
    all_days = [start_date + timedelta(days=i) for i in range(delta.days + 1)]
    
    # Count business days (Monday=0, Sunday=6)
    business_days = 0
    for day in all_days:
        if day.weekday() < 5 and day not in holiday_dates:
            business_days += 1
    
    return business_days
