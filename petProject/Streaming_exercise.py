"""
You are given a streaming dataset in the form of a list of integers. Each element in this list is a list of
streaming minutes watched within a given day. Assume all these minutes belong to a single customer.

[[50, 100, 90], [130], [120, 130], [85], [300, 240, 200, 290], [410, 125], [91, 80, 320], [300, 220]]

You are required to compute following metrics:
1. Compute the total number of days when the customer binged watched? (see below for definition of binge-watching)
2. What is the average duration of binge-watching for the days when the customer did binge-watch? Output the answer
in a dictionary with key being the index belonging to day when the session occurred and value being the average duration.
3. Are there any days when the customer binge-watched in consecutive days? If yes, output that the
number of times such binge-watching pattern occurred. If there is no such answer, output -1.

Binge-wacthing defintion: When each viewing session within a single day is above minutes
and there are atleast two such sessions within the same day.

Bonus points if you can write some test cases with the assumption that the number of binge-watched session in any
dataset will be at minimum one.

Follow-up question:
- How would your approach be if this is a streaming dataset?
"""
import unittest

session_info = [
    [50, 100, 90],
    [130],
    [120, 130],
    [85],
    [300, 240, 200, 290],
    [410, 125],
    [91, 80, 320],
    [300, 220],
]
num_days_binge_watched, consecutive_days_binge_watched = 0, 0
binge_watch_flag = False
avg_binge_watch = {}

for idx in range(len(session_info)):
    values = session_info[idx]
    if len(values) > 1 and min(values) >= 100:
        num_days_binge_watched += 1
        avg_binge_watch[idx] = round(sum(values) / len(values), 2)
        if idx == 0:
            binge_watch_flag = True
        if binge_watch_flag:
            consecutive_days_binge_watched += 1
        binge_watch_flag = True
    else:
        binge_watch_flag = False


class TestMyComputation(unittest.TestCase):
    def test_binge_watched_sessions(self):
        self.assertGreater(num_days_binge_watched, 0)

    def test_avg_binge_watched(self):
        self.assertGreater(len(avg_binge_watch), 0)


if __name__ == "__main__":
    unittest.main()
