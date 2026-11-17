# Railway Booking and Process Scheduler – Queue & Mutual Exclusion

Simulates a railway reservation system where many passengers request seats concurrently.

- **Queue**: requests are stored in a FIFO queue and served in arrival order (FCFS).
- **Mutual exclusion**: dequeue + seat check + seat allocation run inside a `threading.Lock` critical section, so no seat is ever allocated twice.
- **Output**: booking queue, seat allocation status, successful/rejected requests, final verification.

## Run
```bash
python railway_booking.py                       # defaults: 10 seats, 20 requests, 4 workers
python railway_booking.py --seats 8 --requests 15 --workers 3 --seed 42
python -m unittest discover tests -v            # run tests
```
Requires Python 3.8+ (no external libraries).

## Files
| File | Purpose |
|---|---|
| `railway_booking.py` | Main program |
| `tests/test_booking.py` | Unit and concurrency tests |
