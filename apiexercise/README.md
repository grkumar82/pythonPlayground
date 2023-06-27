## READ JSON API

- Language            :   Python
- Operating system    :   Mac OS

#### Setup

Install virtualenv and setup apiexercise in this folder
```
virtualenv apiexercise
source apiexercise/bin/activate
```

Install all the requirement from the txt file
```
pip install -r requirements.txt
```

Run the two unittests:
```
python3 test_util.py
``` 

To view final results from reading the API run:
```
python3 read_process_api.py
```
- Final result
```
{'Helena Fernandez': 11284.12, 'John Oliver': -47517.25, 'Francesco De Mello': 26334.58, 'Bob Martin': -15404.39}
```