
import requests
from dateutil import parser
import json
from datetime import datetime, timezone
import time
import sys
import random
import uuid
import copy


# --------------------------------------------------------------------


class RoundTripEncoder(json.JSONEncoder):
    DATE_FORMAT = "%Y-%m-%d"
    TIME_FORMAT = "%H:%M:%S.%f"

    def default(self, obj):
        if isinstance(obj, datetime):
            return {
                "_type": "datetime",
                "value": obj.strftime("%s %s" % (self.DATE_FORMAT, self.TIME_FORMAT)),
            }
        return super(RoundTripEncoder, self).default(obj)


class RoundTripDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(
            self, object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, obj):
        if "_type" not in obj:
            return obj
        type = obj["_type"]
        if type == "datetime":
            return parser.parse(obj["value"])
        return obj


def call_cloud_function(url, headers, data):
    """
    Calls the cloud function at url with headers and data passed as part of the POST.
    Returns JSON response, passed through RoundTripDecoder
    """

    response_data = None

    try:
        response = requests.post(url=url, data=data, headers=headers)
        response_data = json.loads(response.text, cls=RoundTripDecoder)
    except Exception as e:
        print("ERROR in call_cloud_function: {}".format(str(e)))

    return response_data


class MonkeeTracker:  # --------------------------------------------------------------------
    def __init__(self, db, app_name, function_name, human_uid):
        self.tracker = []
        self.db = db
        self.app_name = app_name
        self.human_uid = human_uid
        self.function_name = function_name

    def set_db(self, db_destination):
        self.db = db_destination

    def set_human_uid(self, human_uid):
        self.human_uid = human_uid

    def track(self, event_name):
        self.tracker.append(
            {"ts": datetime.now(timezone.utc), "e": event_name})

    def persist(self, logUid=None):
        self.track("persist_tracker")
        if len(self.tracker) >= 1:
            diff = self.tracker[-1]["ts"] - self.tracker[0]["ts"]
        overall_diff_s = diff.seconds + diff.microseconds / 1000000

        ti = 0
        while ti < len(self.tracker) - 1:
            next_time = self.tracker[ti + 1]["ts"]
            elapsed_mcs = next_time - self.tracker[ti]["ts"]
            self.tracker[ti]["elapsed_time"] = (
                elapsed_mcs.seconds + elapsed_mcs.microseconds / 1000000
            )
            self.tracker[ti]["a_perc"] = (
                str(
                    int(
                        round(
                            100 *
                            self.tracker[ti]["elapsed_time"] /
                            overall_diff_s, 0
                        )
                    )
                )
                + "%"
            )
            ti += 1

        track_dict = {
            "humanUid": self.human_uid,
            "calledFrom": self.function_name,
            "duration": overall_diff_s,
            "log": self.tracker,
        }
        r = int(10000 * random.random())
        seconds = 9999999999 - time.time()
        logUid = str(seconds) + str(r)
        if logUid is None:
            self.db.document().set(track_dict)
        else:
            self.db.document(logUid).set(track_dict)


def get_size(json_obj):
    """
    returns the size of the JSON object in bytes
    """
    dumps = json.dumps(json_obj, cls=RoundTripEncoder)
    size_bytes = sys.getsizeof(dumps)
    return size_bytes


def dateDiff(unit, ts1, ts2):
    """
    returns the time delta between ts1 and ts2 in the provided unit.
    Unit in: ['second','minute','hour','day']
    """
    elapsedTime = ts2 - ts1
    totalSeconds = elapsedTime.total_seconds()

    if unit in ["s", "sec", "second"]:
        return totalSeconds
    elif unit in ["mn", "min", "minute"]:
        return totalSeconds / 60
    elif unit in ["hr", "hour"]:
        return totalSeconds / 60 / 60
    elif unit in ["d", "day"]:
        return totalSeconds / 60 / 60 / 24


def getval(dictionary, key, default_value=None):
    if dictionary is not None:
        if key in dictionary:
            ret = dictionary[key]
        else:
            ret = default_value
    else:
        ret = default_value
    return ret


def get_uuid():
    return str(uuid.uuid4())


def describe_time(hh_ago):
    ret = ""
    hh_ago = int(round(hh_ago))
    if hh_ago == 0:
        ret = "very recently"
    elif hh_ago == 1:
        ret = "an hour ago"
    elif hh_ago <= 24:
        ret = str(hh_ago) + " hours ago"
    elif hh_ago <= 48:
        ret = "yesterday"
    else:
        dd_ago = int(round(hh_ago / 24))
        ret = str(dd_ago) + " days ago"

    return ret


def makeAscendingUid():
    """
    Creates a uid such that new uids are always alphabetically in front of older ones.
    For typical use in creating FB doc UIDs such that new docs will show up at the top of the collection
    """
    docUid = str(1625607464 * 3 - int(time.time()))
    uuid = get_uuid()
    return f'{docUid}_{uuid}'
