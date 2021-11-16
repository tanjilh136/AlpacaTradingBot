from datetime import datetime, timezone, timedelta
from dateutil import tz
import pytz


class CustomTimeZone:
    MACHINE_TIME_ZONE = ""
    STOCK_MARKET_LOCATION = "America/New_York"
    CLIENT_LOCATION = "America/Los_Angeles"

    def __init__(self, time_zone="America/New_York"):
        """
        if time_zone == MACHINE_TIME_ZONE or "", then the current machine timezone will be considered
        :param time_zone:
        """
        self.time_zone = time_zone
        if (self.time_zone not in pytz.common_timezones) and (self.time_zone != self.MACHINE_TIME_ZONE):
            raise Exception(f"Wrong time zone : ({self.time_zone}) Valid time zones are: \n {pytz.common_timezones}")

    def get_utc_human_time(self,stamp):
        timestamp = stamp / 1000
        dt_object = datetime.fromtimestamp(timestamp)
        dt_object = dt_object.astimezone(pytz.utc)
        return dt_object

    def get_current_utc_iso_date(self):
        utc_now = pytz.utc.localize(datetime.utcnow())
        return utc_now.date().isoformat()

    def get_current_utc_iso_time(self):
        utc_now = pytz.utc.localize(datetime.utcnow())
        return utc_now.time().strftime("%H:%M:%S")

    def get_current_utc_iso_time_date_tuple(self):
        utc_now = pytz.utc.localize(datetime.utcnow())
        return utc_now.time().strftime("%H:%M:%S"), utc_now.date().isoformat()

    def get_utc_stamp(self, y, m, d, h, mi, s, plusday=0):
        date = datetime(y, m, d, h, mi, s) + timedelta(days=plusday)
        timestamp = date.replace(tzinfo=timezone.utc)
        return int(timestamp.timestamp())

    def get_n_min_prev_unix(self, nano_unix, n):
        """
        Reduce n minutes from given nano_unix and return the calculated nano_unix
        :param nano_unix : the base timestamp in nano second
        :param n : number of minutes that needs to be reduced from nano_unix
        :returns n minutes previous timestamp based on nano_unix param
        """
        return nano_unix - (n * 60000000000)

    def get_n_min_next_unix(self, nano_unix, n):
        """
        Add n minutes with given nano_unix and return the calculated nano_unix
        :param nano_unix : the base timestamp in nano second
        :param n : number of minutes that needs to be added with nano_unix
        :returns: n minutes next timestamp based on nano_unix param
        """
        return nano_unix + (n * 60000000000)

    def get_current_unix_time(self):
        """
        Current UTC/UNIX timestamp in nanosecond. Python datetime api returns timestamp
        in second format as float.
        :return: UNIX timestamp in nano (lossy nano second is not precise)
        """
        return int(datetime.now().timestamp() * 1000000000)

    def get_current_unix_time_in_mili(self):
        return int(datetime.now().timestamp() * 1000)

    def reduce_n_day_from_iso_date(self, n, iso_date):
        """
        Subtract n day from given iso_date. It is considered that all params are valid

        :param n: total days to be subtracted
        :param iso_date: iso date from which the days will be subtracted
        :return: iso date as string
        """
        d_parts = datetime.fromisoformat(iso_date)
        reduced_date = datetime(year=d_parts.year, month=d_parts.month, day=d_parts.day) - timedelta(days=n)
        return str(reduced_date)

    def get_date_time_from_nano_timestamp(self, nano_unix):
        """
        Datetime is generated for specified time zone only
        :param nano_unix:
        :return:
        """
        from_zone = tz.tzutc()
        if self.time_zone != self.MACHINE_TIME_ZONE:
            to_zone = tz.gettz(self.time_zone)
        else:
            to_zone = tz.tzlocal()

        return datetime.fromtimestamp(nano_unix / 1000000000, tz=to_zone)

    def get_tz_date_from_nano_unix(self, nano_unix):
        date_time = self.get_date_time_from_nano_timestamp(nano_unix)
        return date_time.date().isoformat()

    def get_tz_time_from_nano_unix(self, nano_unix):
        date_time = self.get_date_time_from_nano_timestamp(nano_unix)
        return date_time.time().isoformat()

    def get_tz_time_date_tuple_from_nano_unix(self, nano_unix, strf="%H:%M:%S"):
        date_time = self.get_date_time_from_nano_timestamp(nano_unix)
        return date_time.time().strftime(strf), date_time.date().isoformat()

    def get_strf_tz_time_from_nano_unix(self, nano_unix, strf="%H:%M:%S"):
        date_time = self.get_date_time_from_nano_timestamp(nano_unix)
        return date_time.time().strftime(strf)

    def get_current_iso_date(self):
        """
        Calculates current date of a specific timezone
        if the timezone is "" or empty string then current machines current date is returned

        :raises Exception when given time zone is invalid:
        :returns: iso date
        """
        if self.time_zone == self.MACHINE_TIME_ZONE:
            # return current date of machine
            return datetime.now().strftime("%Y-%m-%d")
        else:
            # Time zone is valid
            utc_now = pytz.utc.localize(datetime.utcnow())

            tz_time_now = utc_now.astimezone(pytz.timezone(self.time_zone))
            return tz_time_now.date().isoformat()

    def get_current_iso_time_date_tuple(self):
        """
        Calculates current date of a specific timezone
        if the timezone is "" or empty string then current machines current date is returned

        :raises Exception when given time zone is invalid:
        :returns: iso date
        """
        if self.time_zone == self.MACHINE_TIME_ZONE:
            # return current date of machine
            curr_time_date = datetime.now()

            return curr_time_date.time().strftime("%H:%M:%S"), curr_time_date.date().strftime("%Y-%m-%d")
        else:
            # Time zone is valid
            utc_now = pytz.utc.localize(datetime.utcnow())

            tz_time_now = utc_now.astimezone(pytz.timezone(self.time_zone))
            return tz_time_now.time().strftime("%H:%M:%S"), tz_time_now.date().strftime("%Y-%m-%d")

    def get_tz_time_date_from_timestamp(self, timestamp_ms):
        timestamp_ms /= 1000
        to_zone = tz.gettz(self.time_zone)
        result = datetime.fromtimestamp(timestamp_ms, tz=to_zone)
        return result.time().strftime("%H:%M:%S"), result.date().strftime("%Y-%m-%d")

    def date_from_iso(self, iso_date: str) -> datetime:
        return datetime.fromisoformat(iso_date)

    def get_reduced_datetime(self, from_iso_date: str, from_iso_time: str, total_day_to_reduce=0, total_hr_to_reduce=0,
                             total_min_to_reduce=0, total_sec_to_reduce=0):
        y, m, d = from_iso_date.split("-")
        hr, minute, sec = from_iso_time.split(":")
        current_datetime = datetime(year=int(y), month=int(m), day=int(d), hour=int(hr),
                                    minute=int(minute), second=int(sec))
        target_datetime = current_datetime - timedelta(days=total_day_to_reduce, hours=total_hr_to_reduce,
                                                       minutes=total_min_to_reduce,
                                                       seconds=total_sec_to_reduce)
        return target_datetime

    def get_date_time_tuple_from_datetime(self, dt: datetime):
        return dt.date().strftime("%Y-%m-%d"), dt.time().strftime("%H:%M:%S")

    def day_name_from_dt(self, dt: datetime) -> str:
        return dt.strftime("%A")

    def day_name_from_iso(self, iso_date: str) -> str:
        dt = self.date_from_iso(iso_date)
        return dt.strftime("%A")

    def is_day_name_correct(self, date: datetime, day_name="Friday"):
        """
        Detects if the specified date is meets the given day_name
        Allowed days are [Monday,Tuesday, Wednesday, Thursday, Friday, Saturday, Sunday]
        :param day_name:
        :param date:
        :return:
        """
        day = self.day_name_from_dt(date)
        if day.lower() == day_name.lower():
            return True
        else:
            return False

    def get_iso_date_from_datetime(self, date_time: datetime):
        return date_time.date().strftime("%Y-%m-%d")

    def get_recent_date_of_day_name(self, iso_date: str, target_day_name="Friday"):
        """
        date is adjusted to recent target_day_name. If the specified date is in already the target_day_name,
        then no change is applied
        :param iso_date:
        :param target_day_name:
        :return: iso_date of the required day_name
        """
        date = self.date_from_iso(iso_date)
        while not self.is_day_name_correct(date, day_name=target_day_name):
            date = date - timedelta(days=1)
        return self.get_iso_date_from_datetime(date)

    def get_next_date_of_day_name(self, iso_date: str, target_day_name="Saturday"):
        """
        date is adjusted to recent target_day_name. If the specified date is in already the target_day_name,
        then no change is applied
        :param iso_date:
        :param target_day_name:
        :return: iso_date of the required day_name
        """
        date = self.date_from_iso(iso_date)
        while not self.is_day_name_correct(date, day_name=target_day_name):
            date = date + timedelta(days=1)
        return self.get_iso_date_from_datetime(date)

if __name__ == "__main__":
    custom_time = CustomTimeZone()
    from Analyzer import DateRangeCreator

    print(custom_time.get_next_date_of_day_name("2020-01-06", "Sunday"))
    for date_ in DateRangeCreator(start_date="2020-01-06", end_date="2020-01-12"):
        print(custom_time.day_name_from_iso(date_))
