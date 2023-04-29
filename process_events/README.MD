I have solved this problem in **two** ways:
1. Using * *Python* * only: **process_events.py**
2. Using * *PySpark* * only: **pyspark_process_events.py**
**Note** about using PySpark: In a production environment, I’d lean towards using Spark since it has built-in capabilities to process huge amounts of data in relatively short period of time. Spark streaming can also be leveraged if the requirements require real time processing.

**Assumptions I made about this dataset**:
1. I assumed events with `event_type == ‘INGEST-WITH-PROXY’` are actual test data so excluded them from the final result.
2. Customer_id was considered to be valid only if it is of length 32 characters. Included a test case for that.
3. While majority of transaction_ids were of length 36, there were some edge cases:
Lenth of ID	 	Transaction_ID example
  36 				    401938ed-f16c-407a-9c99-e7132856e7bf
  38 				    f87a2632-28d7-4033-ad56-8f03fa2b7689-0
  6 				    385978
  66 				    37983ddc-853b-45fa-8982-7fadd5add9a3_us-east4_2021-03-01T18:33:20Z
  29 				    alice-testing-proxy-ingest-15
  4 				    qwer
  5 				    test1
  28				    Wed Mar  3 12:39:35 PST 2021
  67 				    60df6801-e7b9-49fe-9e0e-ba404f19115d_us-east-1_2021-03-03T22:11:22Z
While some of them look very suspicious, I * *chose* * include all of them except `event_type == ‘INGEST-WITH-PROXY’` since I don’t have proper context if there are actually faulty data. The timestamps associated with the data look ok so I proceeded to include them.

**Additional notes**:
1. The results of this analysis should be available at local host: http://localhost:8080/processed_events.json 
2. For security purposes, I have above HTTO automatically shut down 10 minutes post becoming live.
3. I also have the output in processed_events.json file. 
4. Formatted the Python packages using black and isort packages.
