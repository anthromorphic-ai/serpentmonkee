# _METADATA_:Version: 15
# _METADATA_:Timestamp: 2021-06-18 16:11:18.297375+00:00
# _METADATA_:MD5: f05813b3e6cbf0196dbacc4eec8fbdb1
# _METADATA_:Publish:                                                                                None


# _METADATA_:
from datetime import datetime, timedelta, timezone
import time
import logging
import re


class MonkeeRedis:
    """
    Wrapper for dealing with redis.
    - fb_db:  Firebase client. if None, session_id checks cannot be done
    - cfName: the name of the calling CF. If None, checks cannot be done for the last CF call
    """

    def __init__(
        self, fb_db, cfName, redisClient, appUid='', userUid='', inDebugMode=False
    ):
        self.fb_db = fb_db
        self.callingCF = cfName
        self.redis = redisClient
        self.userUid = userUid
        self.appUid = appUid
        self.inDebugMode = inDebugMode
        self.sessionId = None
        self.timestamp = datetime.now(timezone.utc)
        self.minsBetweenSessions = 30
        self.timeSinceLastEvent = None

    def dprint(self, stringg):
        if self.inDebugMode:
            print(stringg)

    def set_play_event(self, contentUid, playTime):
        """
        Sets the time that the content was played/started
        """
        self.dprint(
            "setting set_play_event = {} - {}".format(contentUid, playTime))
        fieldName = "ContentPlay={}".format(contentUid)
        self.set_project_human_val(
            fieldName=fieldName, dataType="datetimeNTZ", value=playTime, expireInSeconds=60 * 60
        )

    def kill_play_event(self, contentUid):
        fieldName = "ContentPlay={}".format(contentUid)
        self.kill_project_human_val(fieldName=fieldName)

    def get_play_event(self, contentUid):
        """
        Gets the last time playEvent for this content for this user, assuming that it's still in Redis
        """
        self.dprint("getting set_play_event = {} ".format(contentUid))
        fieldName = "ContentPlay={}".format(contentUid)
        return self.get_project_human_val(fieldName=fieldName)

    def get_time_in_content(self, lastPlayTime, endTime):
        """
        Calcs the time-in-seconds spent in the content
        """

        if lastPlayTime is None:
            self.dprint(
                "get_time_in_content ={}.".format("None"))
            return None
        else:
            timeDiff = endTime - lastPlayTime
            self.dprint("get_time_in_content ={}".format(
                timeDiff.total_seconds()))
            return timeDiff.total_seconds()

    def set_last_cf_call(self, eventTime):
        """Sets the last time that this CF was called in this project by this particular user"""
        if self.callingCF:
            self.dprint("setting last_cf_call = {}".format(self.callingCF))
            fieldName = "CFcall={}".format(self.callingCF)
            self.set_project_human_val(
                fieldName=fieldName, dataType="datetimeNTZ", value=eventTime, expireInSeconds=60 * 60 * 24 * 30
            )
        else:
            self.dprint("set_last_cf_call: no CF name specified")

    def get_last_cf_call(self):
        """Gets the last time that this CF was called in this project by this particular user"""
        if self.callingCF:
            fieldName = "CFcall={}".format(self.callingCF)
            return self.get_project_human_val(fieldName=fieldName)
        return None

    def get_sec_since_last_cf_call(self, eventTime):
        llt = self.get_last_cf_call()

        if llt is None:
            self.dprint(
                "get_sec_since_last_cf_call ={}. Setting it now".format("None"))
            self.set_project_human_val(
                fieldName="lastLogTime", dataType="datetimeNTZ", value=eventTime
            )

            return 100
        else:
            if not eventTime.tzinfo and not llt.tzinfo:
                timeDiff = eventTime - llt
            else:
                timeDiff = self.timestamp - llt
            self.dprint("get_sec_since_last_cf_call ={}".format(
                timeDiff.total_seconds()))
            return timeDiff.total_seconds()

    def kill_project_human_val(self, fieldName):
        """
        Removes the given key from Redis
        """
        key = self.appUid + ":" + self.userUid + ":" + fieldName
        if self.redis is not None:
            self.redis.delete(key)

    def set_project_human_val(self, fieldName, dataType, value, expireInSeconds=None):
        """Sets the compound key [self.appUid + ":" + self.userUid + ":" + fieldName] to
        value [dataType + "|" + str(value)]

        - dataType one of ["datetime", "datetimeNTZ", "int", "str"]. 
        """
        key = self.appUid + ":" + self.userUid + ":" + fieldName
        val = dataType + "|" + str(value)
        self.dprint("set_project_human_val: key={}, val={}".format(key, val))
        if self.redis is not None:
            self.redis.set(key, val)
            if expireInSeconds is not None:
                self.redis.expire(key, expireInSeconds)

    def get_project_human_val(self, fieldName):
        """Gets the compound key [self.appUid + ":" + self.userUid + ":" + fieldName]
        from redis and casts it back to native based on the [dataType] used in its initial storing
        """
        if self.redis is not None:
            key = self.appUid + ":" + self.userUid + ":" + fieldName
            val = self.redis.get(key)
            if val is None or val == "None":
                return None
            else:
                val = val.decode("utf-8")
            splitted = val.split("|")
            self.dprint("get_project_human_val splitted: {} ".format(splitted))
            dataType = splitted[0]
            value = splitted[1]
            self.dprint(
                "get_project_human_val: {} = {} ({})".format(
                    key, value, dataType)
            )
            # if value[-6]='+' and value[-3]==":" and dataType='datetime':

            return self.format_val(dataType, value)
        else:
            return None

    def format_val(self, dataType, val):
        """Formats the [val] value given the [dataType]"""
        if val is None:
            return None
        try:
            if dataType in ["datetime", "datetimeNTZ"]:
                fmt = self.inferDTFormat(val)
                return datetime.strptime(val, fmt)
            elif dataType == "int":
                return int(val)
            else:
                return val
        except ValueError as e:
            logging.error(repr(e))
            return None

    def get_session_id(self):
        self.sessionId = self.get_project_human_val("sessionId")
        self.dprint("get_session_id={}".format(self.sessionId))
        return self.sessionId

    def calc_session_id(self, time_diff):
        self.get_session_id()
        if self.sessionId is None:
            self.sessionId = 1
            self.set_session_id()
            return self.sessionId, None, None
        elif time_diff > self.minsBetweenSessions * 60:
            self.sessionId += 1
            self.dprint("incrementing session ID to {}".format(self.sessionId))
            self.set_session_id()
            self.timeSinceLastEvent = time_diff
            return self.sessionId, self.timeSinceLastEvent, True
        elif time_diff <= self.minsBetweenSessions * 60:
            self.dprint("keeping session ID as {}".format(self.sessionId))
            self.timeSinceLastEvent = time_diff
            return self.sessionId, self.timeSinceLastEvent, False

    def set_session_id(self):
        self.set_project_human_val("sessionId", "int", self.sessionId)

        if self.fb_db and self.sessionId >= 1 and len(self.userUid) > 2:
            humanResultRef = self.fb_db.collection(
                "apps", self.appUid, "humans", self.userUid, "results"
            ).document("session")

            humanResultRef.set(
                {
                    "sessionNumber": self.sessionId,
                    "src": "MonkeeRedis from monkee_log_v2",
                    "session": {
                        "tag": "session" + str(self.sessionId),
                        "answeredAt": self.timestamp,
                        "src": "MonkeeRedis from monkee_log_v2"
                    },
                },
                merge=True,
            )

    def inferDTFormat(self, dtString):
        dtFormats = []
        dtFormats.append({"re": re.compile(
            r"\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\.\d*\+\d{2}:\d{2}$"), "fmt": "%Y-%m-%d %H:%M:%S.%f%z"})
        dtFormats.append({"re": re.compile(
            r"\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\.\d*$"), "fmt": "%Y-%m-%d %H:%M:%S.%f"})
        dtFormats.append({"re": re.compile(
            r"\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}$"), "fmt": "%Y-%m-%d %H:%M:%S"})

        for f in dtFormats:
            if len(re.findall(f["re"], dtString)) > 0:
                return f["fmt"]

        return None
