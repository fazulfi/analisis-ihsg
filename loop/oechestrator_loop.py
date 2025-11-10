import time
import schedule
from runner.check_and_dispatch import check_and_send

def job():
    print("Running scheduled dispatch...")
    tickers = ['^JKSE', 'BBCA.JK', 'TLKM.JK']
    for t in tickers:
        try:
            check_and_send(t)
            time.sleep(5)
        except Exception as e:
            print(f"Error for {t}:", e)

schedule.every(30).minutes.do(job)

print("Scheduler started, press Ctrl+C to stop.")
job()  # run first time immediately
while True:
    schedule.run_pending()
    time.sleep(1)
