import copy
import dataclasses
import datetime
import json
import time
from pathlib import Path
from threading import Thread
from typing import Dict, List

from trader import OrderData

class InvalidAggregateScale(BaseException):
    """Raised when Invalid Aggerage Scale is provided"""
    pass


class AggregateMaker:
    """Receives second data and converts it into aggregate data"""

    def __init__(self, scale="minute"):
        allowed_scales = {"minute": 60, "second": 1, "hour": 3600}
        scale = scale.lower()
        if scale not in allowed_scales:
            raise InvalidAggregateScale(f"Scale must be from {list(allowed_scales.keys())}")
        self.scale_second = allowed_scales[scale]


class TimePipe:
    """Keeps only specified timestamp data"""

    def __init__(self, keep_minutes: 19):
        self.h_total_min_ms = keep_minutes * 60 * 1000
        self.minute_data = []

    def clear_before_timestamp_data(self, timestamp):
        new_data = []
        for data, stamp in self.minute_data:
            if stamp > timestamp:
                new_data.append((data, stamp))
            else:
                pass
                # print(f"Cleared: {data, stamp}")
        self.minute_data = new_data

    def push(self, data, timestamp_ms):
        # print(f"Pushing {data}")
        self.clear_before_timestamp_data(timestamp_ms - self.h_total_min_ms)
        self.minute_data.append((data, timestamp_ms))

    def clear(self):
        self.minute_data.clear()

    def get_all_data(self, include_timestamp=False):
        if not include_timestamp:
            res = [data for data, stamp in self.minute_data]
            return copy.deepcopy(res)
        return copy.deepcopy(self.minute_data)

    def get_lowest_data_if_dict(self, key='l'):
        lowest = 100000000000
        temp_data = None
        for data, stamp in self.minute_data:
            if data[key] < lowest:
                lowest = data[key]
                temp_data = data
        return temp_data

    def get_total_addition_if_dict(self, min_timestamp, max_timestamp, key='v'):
        total = 0
        for data, stamp in self.minute_data:
            if min_timestamp <= stamp <= max_timestamp:
                total += data[key]
        return total


class TimedStorage:
    """Deletes the stored data after a specified minute"""

    def __init__(self):
        self.data = []
        self.deleted = []
        self.carry_on = True
        t1 = Thread(target=self.__start_countdown)
        t1.start()

    def __start_countdown(self):
        while self.carry_on:
            time.sleep(60)
            for i in range(len(self.data) - 1, -1, -1):
                self.data[i]['elapsed'] += 1
                if self.data[i]['elapsed'] == self.data[i]['delete_after_min']:
                    print(f"Deleting {self.data[i]}")
                    self.deleted.append(self.data[i])
                    del self.data[i]

    def pause_countdown(self):
        self.carry_on = False

    def push(self, id, data, delete_after_min=1):
        print(f"Pushing {id}")
        self.data.append({"id": id, "data": data, "elapsed": 0, "delete_after_min": delete_after_min})

    def get_data(self):
        return [d['data'] for d in copy.deepcopy(self.data)]

    def get_ids(self):
        return [d['id'] for d in self.data]

    def get_id_and_data(self):
        return [{"id": d['id'], "data": d['data']} for d in self.data]

    def get_id_and_data_and_deleted(self):
        """
        Return Valid and recently Deleted data and clears deleted data
        """
        res = {"valid": [{"id": d['id'], "data": d['data']} for d in self.data],
               "expired": [{"id": d['id'], "data": d['data']} for d in self.deleted]}
        self.deleted = []
        return res


class TimeRangeCreator:
    """Create list of time in 24 hr format in the given time range"""

    def __init__(self, start_time="01:00:00", end_time="04:02:00", interval_sec=60):
        self.start_hr, self.start_min, self.start_sec = start_time.split(":")
        self.start_stamp = (int(self.start_hr) * 3600) + (int(self.start_min) * 60) + int(self.start_sec)
        self.end_hr, self.end_min, self.end_sec = end_time.split(":")
        self.end_stamp = (int(self.end_hr) * 60 * 60) + (int(self.end_min) * 60) + int(self.end_sec)
        self.interval_sec = int(interval_sec)

    def get_list(self):
        """List of times in the range created using interval seconds"""

        times = []
        if self.start_stamp <= self.end_stamp:
            for time_sec in range(self.start_stamp, self.end_stamp + 1, self.interval_sec):
                hr = int(time_sec / 3600)
                minute = int((time_sec % 3600) / 60)
                sec = time_sec % 60
                times.append(f"{hr:02d}:{minute:02d}:{sec:02d}")
        else:
            start_again_from = 0
            pause_before = 24 * 3600

            # For pm to am switching
            for time_sec in range(self.start_stamp, pause_before, self.interval_sec):
                hr = int(time_sec / 3600)
                minute = int((time_sec % 3600) / 60)
                sec = time_sec % 60
                times.append(f"{hr:02d}:{minute:02d}:{sec:02d}")
                if (time_sec + self.interval_sec) >= pause_before:
                    start_again_from = (time_sec + self.interval_sec) - pause_before

            # pm to am switched
            for time_sec in range(start_again_from, self.end_stamp + 1, self.interval_sec):
                hr = int(time_sec / 3600)
                minute = int((time_sec % 3600) / 60)
                sec = time_sec % 60
                times.append(f"{hr:02d}:{minute:02d}:{sec:02d}")
        return times

    def get_dict(self):
        """Dict of times in the range created using interval seconds"""

        times = {}
        if self.start_stamp <= self.end_stamp:
            for time_sec in range(self.start_stamp, self.end_stamp + 1, self.interval_sec):
                hr = int(time_sec / 3600)
                minute = int((time_sec % 3600) / 60)
                sec = time_sec % 60
                times[f"{hr:02d}:{minute:02d}:{sec:02d}"] = True
        else:
            start_again_from = 0
            pause_before = 24 * 3600

            # For pm to am switching
            for time_sec in range(self.start_stamp, pause_before, self.interval_sec):
                hr = int(time_sec / 3600)
                minute = int((time_sec % 3600) / 60)
                sec = time_sec % 60
                times[f"{hr:02d}:{minute:02d}:{sec:02d}"] = True
                if (time_sec + self.interval_sec) >= pause_before:
                    start_again_from = (time_sec + self.interval_sec) - pause_before

            # pm to am switched
            for time_sec in range(start_again_from, self.end_stamp + 1, self.interval_sec):
                hr = int(time_sec / 3600)
                minute = int((time_sec % 3600) / 60)
                sec = time_sec % 60
                times[f"{hr:02d}:{minute:02d}:{sec:02d}"] = True
        return times


def create_required_folder(path_dir):
    try:
        Path(path_dir).mkdir(parents=True, exist_ok=True)
    except:
        print("Directory already exists")


@dataclasses.dataclass
class SellData:
    symbol: str
    sell_price: float
    timestamp: int
    profit: float
    quantity: int


if __name__ == "__main__":
    # x: Dict[str,SellData] = {}
    # x['aapl'] = SellData(symbol="None",sell_price=12,timestamp=1,profit=12,quantity=23)
    # print(x['aapl'].sell_price)

    d : Dict[str,List[dict]] = {}



