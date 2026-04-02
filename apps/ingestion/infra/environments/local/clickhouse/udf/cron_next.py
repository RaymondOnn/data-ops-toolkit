#!/usr/bin/python3
import sys
from datetime import datetime

from croniter import croniter


# ClickHouse Executable UDFs use standard I/O (TabSeparated by default)
def main():
    for line in sys.stdin:
        parts = line.strip().split("\t")
        if len(parts) < 2:
            print("\\N")
            continue

        cron_expr = parts[0]
        ref_ts_raw = parts[1]

        try:
            # Handle ClickHouse NULL or empty inputs
            if (
                ref_ts_raw == "\\N"
                or not ref_ts_raw
                or ref_ts_raw == "0"
                or ref_ts_raw == "1970-01-01 00:00:00.000"
            ):
                ref_ts = datetime.now()
            elif "-" in ref_ts_raw:
                # Handle YYYY-MM-DD HH:MM:SS.mmm format
                ref_ts_str = ref_ts_raw.split(".")[0]
                ref_ts = datetime.strptime(ref_ts_str, "%Y-%m-%d %H:%M:%S")
            else:
                # Handle Unix Timestamp (Seconds vs Milliseconds/Ticks)
                val = float(ref_ts_raw)
                if val > 1e11:  # Likely milliseconds/ticks for DateTime64(3)
                    val /= 1000
                ref_ts = datetime.fromtimestamp(val)

            iterator = croniter(cron_expr, ref_ts)
            # Return formatted for DateTime64(3)
            print(iterator.get_next(datetime).strftime("%Y-%m-%d %H:%M:%S.000"))
        except Exception:
            print("\\N")  # Return NULL on failure
        sys.stdout.flush()


if __name__ == "__main__":
    main()
