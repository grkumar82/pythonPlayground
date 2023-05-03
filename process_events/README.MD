I solved this problem using Python specifically and the code is available in **process_events.py**

In a production environment, I’d lean towards using **Spark** since it has built-in capabilities to process huge amounts of data in relatively short period of time. Spark streaming can also be leveraged if the requirements require real time processing/analytics. A recent example of me leveraging PySpark to query a publicly available dataset: https://github.com/grkumar82/pythonPlayground/blob/main/petProject/electricCars_WA_state/electric_cars_analysis.py

**Additional notes**:
1. The results of this analysis should be available at local host: http://localhost:8080/processed_events.json 
2. For security purposes, I have above HTTP automatically shut down 10 minutes post becoming live.
3. I also have the output in **processed_events.json** file. 
4. Formatted the python files using **black** and **isort** packages.

**Assumptions I made about this dataset**:
1. I assumed events with `event_type == ‘INGEST-WITH-PROXY’` are actual test data so excluded them from the final result.
2. Customer_id was considered to be valid only if it is of length 32 characters. Included a test case for that.
3. While majority of transaction_ids were of length 36, there were some edge cases:

| Lenth of ID   | Transaction_ID example                                                        |
| ------------- | ----------------------------------------------------------------------------- |
| 66            | 37983ddc-853b-45fa-8982-7fadd5add9a3_us-east4_2021-03-01T18:33:20Z            |
| 36            | 401938ed-f16c-407a-9c99-e7132856e7bf                                          |
| 38            | f87a2632-28d7-4033-ad56-8f03fa2b7689-0                                        |
| 06            | 385978                                                                        |
| 29            | alice-testing-proxy-ingest-15                                                 |
| 04            | qwer                                                                          |
| 05            | test1                                                                         |
| 28            | Wed Mar  3 12:39:35 PST 2021                                                  |
| 67            | 60df6801-e7b9-49fe-9e0e-ba404f19115d_us-east-1_2021-03-03T22:11:22Z           |

While some of them look very suspicious, I * *chose* * to include all of them except `event_type == ‘INGEST-WITH-PROXY’` since I don’t have proper context if there are actually faulty data. The timestamps associated with these transactions did look ok so I proceeded to include them.
