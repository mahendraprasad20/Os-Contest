"""
Railway Booking and Process Scheduler - Queue and Mutual Exclusion
-------------------------------------------------------------------
* Passengers (producer threads) submit booking requests concurrently.
* Requests are stored in a FIFO queue and served strictly FCFS.
* Worker threads (consumers) take the next request and allocate the seat
  inside a critical section (threading.Lock), so a seat is never
  allocated twice.
"""
import argparse
import random
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from itertools import count


@dataclass
class BookingRequest:
    req_id: int
    passenger: str
    seat_no: int
    arrival_time: float = field(default_factory=time.time)


@dataclass
class BookingResult:
    request: BookingRequest
    success: bool
    message: str
    worker: str


class BookingQueue:
    """Thread-safe FIFO queue of booking requests."""

    def __init__(self):
        self._items = deque()
        self._ids = count(1)
        self._lock = threading.Lock()

    def submit(self, passenger, seat_no):
        with self._lock:
            req = BookingRequest(next(self._ids), passenger, seat_no)
            self._items.append(req)  # arrival order == id order
            return req

    def snapshot(self):
        with self._lock:
            return list(self._items)

    def pop_left(self):
        """Caller must already hold the scheduler's critical section."""
        with self._lock:
            return self._items.popleft() if self._items else None

    def __len__(self):
        with self._lock:
            return len(self._items)


class RailwayScheduler:
    def __init__(self, total_seats, process_delay=0.0):
        self.total_seats = total_seats
        self.seats = {n: None for n in range(1, total_seats + 1)}  # seat -> passenger
        self.queue = BookingQueue()
        self.results = []
        self.process_delay = process_delay
        self._cs = threading.Lock()          # critical-section lock
        self._results_lock = threading.Lock()

    def submit(self, passenger, seat_no):
        return self.queue.submit(passenger, seat_no)

    def process_next(self, worker_name):
        """
        Take the next request (FCFS) and allocate the seat.
        Dequeue + check + allocate all happen in ONE critical section, so
        requests are decided in arrival order and no seat is double-booked.
        """
        with self._cs:                                   # ---- ENTER CRITICAL SECTION
            req = self.queue.pop_left()
            if req is None:
                return None
            if req.seat_no not in self.seats:
                res = BookingResult(req, False, f"Seat {req.seat_no} does not exist", worker_name)
            elif self.seats[req.seat_no] is not None:
                res = BookingResult(req, False,
                                    f"Seat {req.seat_no} already booked by {self.seats[req.seat_no]}",
                                    worker_name)
            else:
                if self.process_delay:
                    time.sleep(self.process_delay)       # simulate DB latency (race window)
                self.seats[req.seat_no] = req.passenger
                res = BookingResult(req, True, f"Seat {req.seat_no} allocated", worker_name)
        with self._results_lock:                         # ---- EXIT CRITICAL SECTION
            self.results.append(res)
        return res

    def worker(self, name, stop_event):
        while True:
            res = self.process_next(name)
            if res is None:
                if stop_event.is_set() and len(self.queue) == 0:
                    return
                time.sleep(0.005)

    # ---------- display helpers ----------
    def show_queue(self, title="BOOKING QUEUE"):
        print(f"\n=== {title} ===")
        items = self.queue.snapshot()
        if not items:
            print("(empty)")
        for r in items:
            print(f"  Req#{r.req_id:<3} {r.passenger:<10} -> Seat {r.seat_no}")

    def show_seats(self, title="SEAT ALLOCATION STATUS"):
        print(f"\n=== {title} ===")
        for n, p in self.seats.items():
            print(f"  Seat {n:<3}: {'BOOKED by ' + p if p else 'AVAILABLE'}")

    def show_results(self):
        print("\n=== BOOKING RESULTS (FCFS order) ===")
        for r in self.results:
            tag = "SUCCESS" if r.success else "REJECTED"
            print(f"  Req#{r.request.req_id:<3} {r.request.passenger:<10} Seat {r.request.seat_no:<3} "
                  f"{tag:<8} | {r.message} [{r.worker}]")
        ok = sum(r.success for r in self.results)
        print(f"\nTotal: {len(self.results)} | Successful: {ok} | Rejected: {len(self.results) - ok}")

    def verify_no_duplicates(self):
        booked = [r.request.seat_no for r in self.results if r.success]
        assert len(booked) == len(set(booked)), "Duplicate seat allocation detected!"
        return True


def run(seats=10, requests=20, workers=4, seed=None, delay=0.002):
    rnd = random.Random(seed)
    sched = RailwayScheduler(seats, process_delay=delay)

    # Phase 1: passengers submit requests concurrently (many target same seats)
    def passenger(i):
        sched.submit(f"Passenger{i}", rnd_seats[i])

    rnd_seats = [rnd.randint(1, seats) for _ in range(requests)]
    threads = [threading.Thread(target=passenger, args=(i,)) for i in range(requests)]
    for t in threads: t.start()
    for t in threads: t.join()

    sched.show_queue("BOOKING QUEUE (before processing)")
    sched.show_seats("SEAT STATUS (before processing)")

    # Phase 2: worker threads process the queue FCFS using the critical section
    stop = threading.Event()
    pool = [threading.Thread(target=sched.worker, args=(f"W{i+1}", stop)) for i in range(workers)]
    for t in pool: t.start()
    stop.set()
    for t in pool: t.join()

    sched.show_queue("BOOKING QUEUE (after processing)")
    sched.show_results()
    sched.show_seats("SEAT STATUS (final)")
    sched.verify_no_duplicates()
    print("\nVerification: no duplicate seat allocation occurred.")
    return sched


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Railway booking: queue + mutual exclusion")
    ap.add_argument("--seats", type=int, default=10)
    ap.add_argument("--requests", type=int, default=20)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--seed", type=int, default=None)
    a = ap.parse_args()
    run(a.seats, a.requests, a.workers, a.seed)
