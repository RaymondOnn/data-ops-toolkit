#!/usr/bin/python3
import sys
from datetime import datetime

from croniter import croniter


# ClickHouse Executable UDFs use standard I/O (TabSeparated by default)
def main():
    for line in sys.stdin:
        parts = line.strip().split("\t")
        if len(parts) < 2:
            continue
            
        cron_expr = parts[0]
        # ClickHouse DateTime64 format: YYYY-MM-DD HH:MM:SS.mmm
        ref_ts_str = parts[1].split(".")[0] 
        
        try:
            ref_ts = datetime.strptime(ref_ts_str, "%Y-%m-%d %H:%M:%S")
            iterator = croniter(cron_expr, ref_ts)
            print(iterator.get_next(datetime).strftime("%Y-%m-%d %H:%M:%S"))
        except Exception:
            print("\\N") # Return NULL on failure

if __name__ == "__main__":
    main()
