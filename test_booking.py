import os, sys, threading, unittest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from railway_booking import RailwayScheduler


class TestBooking(unittest.TestCase):
    def test_same_seat_only_one_wins_fcfs(self):
        s = RailwayScheduler(5)
        for name in ["A", "B", "C"]:
            s.submit(name, 3)
        while s.process_next("W1"):
            pass
        self.assertEqual(s.seats[3], "A")
        self.assertEqual([r.success for r in s.results], [True, False, False])

    def test_invalid_seat_rejected(self):
        s = RailwayScheduler(5)
        s.submit("A", 99)
        s.process_next("W1")
        self.assertFalse(s.results[0].success)

    def test_concurrent_no_duplicates(self):
        s = RailwayScheduler(5, process_delay=0.001)
        for i in range(50):
            s.submit(f"P{i}", (i % 5) + 1)
        stop = threading.Event(); stop.set()
        ts = [threading.Thread(target=s.worker, args=(f"W{i}", stop)) for i in range(8)]
        [t.start() for t in ts]; [t.join() for t in ts]
        self.assertTrue(s.verify_no_duplicates())
        self.assertEqual(sum(r.success for r in s.results), 5)
        ids = [r.request.req_id for r in s.results]
        self.assertEqual(ids, sorted(ids))  # FCFS order preserved


if __name__ == "__main__":
    unittest.main()
