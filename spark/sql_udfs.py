def _skip_holidays_sql(ts_col, holidays_list):
    """
    Given a timestamp (e.g., a candidate window like 2026-12-25 10:00:00),
    if that day is a weekend or holiday, find the NEXT business day at 10:00:00.
    This is expressed as native Spark SQL array functions (not a Python UDF) so the
    whole computation runs in the JVM with no Python serialization or round-trip.
    (Python UDFs do work in this pipeline — see _cloudpickle_compat.py, which Job C
    relies on — SQL is simply the better fit here.)
    """
    # Create an array of 10 consecutive days starting from the candidate day
    # Check each day if it's a weekend or holiday
    # Take the first day that is valid
    # If the valid day is NOT the candidate day, the time becomes 10:00:00.
    
    holidays_str = ", ".join(f"'{d}'" for d in holidays_list)
    if not holidays_str:
        holidays_str = "'1900-01-01'" # Dummy holiday if empty
        
    return f"""
    element_at(
        filter(
            transform(
                sequence(0, 10),
                x -> date_add(to_date({ts_col}), x)
            ),
            d -> dayofweek(d) NOT IN (1, 7) AND cast(d as string) NOT IN ({holidays_str})
        ),
        1
    )
    """

def get_pesonet_window_expr(ts_col, holidays_list):
    """
    If the date changes because it was a weekend/holiday, the time must be reset to 10:00:00.
    """
    next_valid_date = _skip_holidays_sql(ts_col, holidays_list)
    
    # If next_valid_date is the same as the original date, keep original time.
    # Otherwise, it's a new day, so the window is 10:00:00.
    return f"""
    CASE 
        WHEN {next_valid_date} == to_date({ts_col}) THEN {ts_col}
        ELSE make_timestamp(year({next_valid_date}), month({next_valid_date}), day({next_valid_date}), 10, 0, 0)
    END
    """
