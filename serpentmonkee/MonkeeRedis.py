from datetime import datetime, timedelta, timezone
import logging


class MonkeeRedis:
    def __init__(
        self, cfName, redisClient, projectName, userUid, humanDocRef, inDebugMode=False
    ):

        self.callingCF = cfName
        self.redis = redisClient
        self.userUid = userUid
        self.projectName = projectName
        self.humanDocRef = humanDocRef
        self.inDebugMode = inDebugMode
        self.sessionId = None
        self.timestamp = datetime.now(timezone.utc)
        self.minsBetweenSessions = 30
        self.timeSinceLastEvent = None

    def dprint(self, stringg):
        if self.inDebugMode:
            print(stringg)

    def set_last_cf_call(self):
        """Sets the last time that this CF was called in this project by this particular user"""
        fieldName = "CFcall={}".format(self.callingCF)
        self.set_project_human_val(
            fieldName=fieldName, dataType="datetime", value=self.timestamp
        )

    def get_last_cf_call(self):
        """Gets the last time that this CF was called in this project by this particular user"""

        fieldName = "CFcall={}".format(self.callingCF)
        print("get_last_cf_call starting for {}".format(fieldName))
        return self.get_project_human_val(fieldName=fieldName)

    def get_sec_since_last_cf_call(self):
        llt = self.get_last_cf_call()

        if llt is None:
            print("get_sec_since_last_cf_call ={}".format("None"))
            return 1000
        else:
            timeDiff = self.timestamp - llt
            print("get_sec_since_last_cf_call ={}".format(
                timeDiff.total_seconds()))
            return timeDiff.total_seconds()

    def set_project_human_val(self, fieldName, dataType, value, expireInSeconds=None):
        """Sets the compound key [self.projectName + ":" + self.userUid + ":" + fieldName] to
        value [dataType + "|" + str(value)]
        """
        key = self.projectName + ":" + self.userUid + ":" + fieldName
        val = dataType + "|" + str(value)
        self.dprint("set_project_human_val: key={}, val={}".format(key, val))
        if self.redis is not None:
            self.redis.set(key, val)
            if expireInSeconds is not None:
                self.redis.expire(key, expireInSeconds)

    def get_project_human_val(self, fieldName):
        """Gets the compound key [self.projectName + ":" + self.userUid + ":" + fieldName]
        from redis and casts it back to native based on the [dataType] used in its initial storing
        """
        if self.redis is not None:
            key = self.projectName + ":" + self.userUid + ":" + fieldName
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
            return self.format_val(dataType, value)
        else:
            return None

    def format_val(self, dataType, val):
        """Formats the [val] value given the [dataType]"""
        if val is None:
            return None
        try:
            if dataType == "datetime":
                return datetime.strptime(val, "%Y-%m-%d %H:%M:%S.%f%z")
            elif dataType == "int":
                return int(val)
            else:
                return val
        except ValueError as e:
            logging.error(repr(e))
            return None

    def get_session_id(self):
        self.sessionId = self.get_project_human_val("sessionId")
        print("get_session_id={}".format(self.sessionId))
        return self.sessionId

    def calc_session_id(self, time_diff):
        self.get_session_id()
        if self.sessionId is None:
            self.sessionId = 1
            self.set_session_id()
            return self.sessionId, None
        elif time_diff > self.minsBetweenSessions * 60:
            self.sessionId += 1
            print("incrementing session ID to {}".format(self.sessionId))
            self.set_session_id()
            self.timeSinceLastEvent = time_diff
            return self.sessionId, self.timeSinceLastEvent
        elif time_diff <= self.minsBetweenSessions * 60:
            print("keeping session ID as {}".format(self.sessionId))
            return self.sessionId, self.timeSinceLastEvent

    def set_session_id(self):
        self.set_project_human_val("sessionId", "int", self.sessionId)

        self.humanDocRef.set(
            {
                "sessionNumber": self.sessionId,
                "session": {
                    "tag": "session" + str(self.sessionId),
                    "answeredAt": self.timestamp,
                },
            },
            merge=True,
        )
