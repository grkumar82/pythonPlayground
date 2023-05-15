This package is for below asks. I created three **run_** files for three tasks, running these files will automatically run unittests as well:
**run_elite_status.py**
**run_flight_routes.py**
**run_three_tiers.py**

# Project Details
# Task List

You may do these tasks in any order, but take note that they are listed in the order your team has prioritized completing them.

Reminder that you are NOT expected to complete all tasks. You are expected to write clean, readable code. Remember to add comments explaining what you were working on if you run out of time in the middle of a task.


## Task 1

Recent user studies have found that customers aren’t satisfied with our current rewards membership tiers. We plan to re-work them to be more appealing, but in order to do so, we need to investigate our data to uncover answers to a few questions.
We’ve pulled three CSV files from our databases for your use in this project:

  - `members_2022.csv`: All of our rewards members as of December 31, 2022. (Note: members with elite status may have earned that status in either 2021 or 2022.)

  - `flights_2022.csv`: All Amazing Airlines flights that departed in 2022.

  - `tickets_2022.csv`: All airline tickets purchased by rewards members for flights that departed in 2022.

These CSV files are included in this directory. Additionally, we've included `csv_database_structure.png`, an image describing the structure of the data. Use this information to complete the following tasks.

---

Our rewards program has two membership tiers: **Amazing Rewards Basic**, and **Amazing Rewards Elite**. Anyone who creates an account on our website instantly becomes a Basic member, which allows them to collect miles in their account, and spend their miles on free flights and seat upgrades. In order to become an Elite member, a customer must fly at least 75,000 miles with Amazing Airlines within a calendar year. (They then retain Elite membership until the end of the following calendar year.)
Some customers have expressed that the 75,000 mile requirement makes Elite status too difficult to achieve. We want to validate this claim. Using the data in `members_2022.csv` and `flights_2022.csv`, please find answers to the following questions:
- What percentage of rewards members had Elite status on December 31, 2022?
    - **TODO:** Add your answer here.
- On average, how many Amazing Airlines flights would a user have to take in a calendar year in order to reach Elite status?
    - **TODO:** Add your answer here.

Please leave the code you wrote for this task in a new file called `elite_status.py (or a Jupyter notebook, if you prefer)`, including comments explaining your process, so that your answers can be reproduced.


## Task 2

The news that we’re analyzing this data has made its way to the route planning team, and they’ve asked us to prepare for them a CSV file containing all of the flight routes flown in 2022. Because their request is high-priority and time-sensitive, we'd like to do this before moving on to our own analysis.

Each Amazing Airlines flight repeats on either a daily or weekly cadence. Using the information in `flights_2022.csv` (which contains a row for each *individual flight* flown in 2022), please create a CSV file called `flight_routes_2022.csv`, which should contain a single row for each *flight route* included in `flights_2022.csv`, with the following columns:

- departure_airport
- arrival_airport
- departure_weekday
- departure_time 
- arrival_time
- distance_miles

`departure_weekday` should be an abbreviated weekday ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", or "Sun") if the flight departs weekly, or "All" if the flight departs daily. `departure_time` and `arrival_time` should be in the `HH:MM:SS` format.

**Important:** There is one route that is flown twice daily, and two routes that are flown twice weekly. Because of the nature of their analysis, the route planning team has asked us to **exclude these routes** from the `flight_routes_2022.csv` dataset entirely. In addition to excluding them from the dataset, can you please note below which routes these are?

- Which route is flown twice daily, and which two routes are flown twice weekly?
    - **TODO:** Add your answer here.


Please leave the code you wrote for this task in a new file called `flight_routes.py (or a Jupyter notebook, if you prefer)`, including comments explaining your process, so that your answers can be reproduced.


## Task 3

Based on user feedback, we’re exploring the introduction of a new, three-tier rewards program. The tiers would be called Amazing Rewards **Bronze**, Amazing Rewards **Silver**, and Amazing Rewards **Gold**. 
As with the Basic tier in the current program, all members will automatically receive Bronze status simply by creating an account. Our goal is for ~50% of our users to *attain* Silver status in a given year, and ~10% of users to also *attain* Gold status. (Note: Because rewards status lasts until the end of the following year, the share of users who *have* a status level in a given year will be somewhat higher than the share who *attain* status that year.)
Using the data in the CSV files, please find an answer to the following question:
- In order to meet our goals, how many miles should a member have to fly in a given year to achieve the Silver and Gold statuses, respectively?
  - **TODO:** Add your answer here.

Additionally, please save a graph displaying the number of miles our users have flown in the past year to `user_distances_graph.png`. There’s no need to spend too much time making it pretty; we’d prefer you spend that time working on the other tasks (you can always come back to this if you have extra time).
Please leave the code you wrote for this task in a new file called `three_tiers.py (or a Jupyter notebook, if you prefer)`, including comments explaining your process, so that your answers can be reproduced.

