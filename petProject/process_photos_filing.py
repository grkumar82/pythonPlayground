"""
This program reads a file that has photos sorted randomly. Return the output in a separate file
Goal is to rename them after the city they were taken and return them in the same order they are processed.
Assume every city has a set of pictures and when you rename them, you should name them based on the order of when they were taken.
Also, the numbering of the pictures for each city should have same length as the number of pics for that city.
So if Seattle has 12 pictures, first picture should be Seattle01.jpg and final picture should be Seattle12.jpeg
Write some test cases for your program. Assume there are at least 2 or more cities in your file and each city
has at least 2 or more pictures.
Assume all timestamps are unique within the set of values for a given city.
Example input:
    space_needle01.jpg, Seattle, 2021-01-01 02:00:08
    space_needle1.jpeg, Seattle, 2021-01-01 01:00:08
output:
    Seattle_02.jpg
    Seattle_01.jpeg
"""

import datetime
import unittest
from collections import OrderedDict, defaultdict


def convert_string_unix_timestamp(input_str):
    """
    :param input_str: str
    :return: unix_timestamp
    Converts a string to a unix timestamp
    """
    timestamp_str = input_str.lstrip().rstrip("\n")
    timestamp_formatted = datetime.datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
    timestamp_unix = datetime.datetime.timestamp(timestamp_formatted)
    return timestamp_unix


class FormatPhotos:
    def __init__(self, input_file, output_file):
        self.cities_timestamp = defaultdict(OrderedDict)
        self.input_file = input_file
        self.output_file = output_file

    def process_file(self):
        lines = self.input_file.readlines()

        for line in lines:
            values = line.split(",")
            city_name = values[1]
            timestamp = convert_string_unix_timestamp(values[-1])
            if city_name in self.cities_timestamp:
                timestamp_cnt = len(self.cities_timestamp[city_name]) + 1
                self.cities_timestamp[city_name][timestamp] = timestamp_cnt
            else:
                self.cities_timestamp[city_name][timestamp] = 1

        for line in lines:
            values = line.split(",")
            file_format = values[0].split(".")[1]
            city_name = values[1]
            timestamp = convert_string_unix_timestamp(values[-1])
            if city_name in self.cities_timestamp:
                max_length_city = str(max(self.cities_timestamp[city_name].values()))
                order_of_picture = self.cities_timestamp[city_name][timestamp]
                picture_number = str(order_of_picture)
                while len(max_length_city) > len(picture_number):
                    picture_number = "0" + picture_number
                photo_file_name = city_name + "_" + picture_number + "." + file_format
                self.output_file.writelines(photo_file_name)
                self.output_file.writelines("\n")


class TestMyComputation(unittest.TestCase):
    def test_input_output(self):
        with open("interval_dates.txt", "r") as file1, open(
            "formatted_photos.txt", "w"
        ) as file2:
            formatter = FormatPhotos(file1, file2)
            formatter.process_file()
        with open("interval_dates.txt", "r") as file1, open(
            "formatted_photos.txt", "r"
        ) as file2:
            self.assertEquals(len(file1.readlines()), len(file2.readlines()))
        file1.close()
        file2.close()


if __name__ == "__main__":
    unittest.main()
