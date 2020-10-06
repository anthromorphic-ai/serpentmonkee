# _METADATA_:Version: 1
# _METADATA_:Timestamp: 2020-10-01 11:59:18.409826+00:00
# _METADATA_:
class GCPDocument:
    def __init__(self, data, context):
        self.data = data
        self.context = context

        path_parts = context.resource.split("/documents/")[1].split("/")
        self.path_parts = path_parts
        self.documentUid = path_parts[-1]
        self.collectionPath = "/".join(path_parts[:-1])
        self.collectionName = path_parts[-2]
        self.projectName = path_parts[1]
        self.humanUid = None

        self.collection_path = path_parts[0]
        self.document_path = "/".join(path_parts[1:])
        self.whatChanged = []

        self.operation = "<unknown>"
        if data["oldValue"] == {} and data["value"] != {}:
            self.operation = "create"
        elif data["oldValue"] != {} and data["value"] == {}:
            self.operation = "delete"
        elif data["updateMask"] != {}:
            self.operation = "update"
            if "fieldPaths" in data["updateMask"]:
                self.whatChanged = data["updateMask"]["fieldPaths"]

        self.dataValue = self.getval(data["value"], "fields", {})
        self.dataOldValue = self.getval(data["oldValue"], "fields", {})

        self.dictValue = {}
        self.dictOldValue = {}

    def getval(self, dictionary, key, default_value=None):
        if dictionary is not None:
            if key in dictionary:
                ret = dictionary[key]
            else:
                ret = default_value
        else:
            ret = default_value
        return ret

    def decide_on_doc_change(self, ignoreIfChangeIsNotIn):
        """Decides if the doc change is worth a graph update.
        If any of the items in whatChanged is in ignoreIfChangeIsNotIn AND the operation is an UPDATE then YEs, else NO
        """
        if self.operation not in ("update"):
            return False

        for wc in self.whatChanged:
            if wc in ignoreIfChangeIsNotIn:
                return True

        return False

    def translateVal(self, fieldName, flatDict):
        if isinstance(flatDict, dict):
            for k in flatDict:
                dataType = k
                val = flatDict[k]
        else:
            dataType = fieldName
            val = flatDict
        if dataType == "doubleValue":
            return float(val)
        elif dataType == "stringValue":
            return str(val)
        elif dataType == "timestampValue":
            return str(val)  # 2020-09-07T14:42:07.635247Z
        elif dataType == "integerValue":
            return int(val)
        elif dataType == "booleanValue":
            return val

    def translateDict2(self, dict_):
        retDict = {}
        for dataType in dict_:
            # print("translateDict2. k={}, type={}".format(dataType, type(dict_[dataType])))
            if dataType not in ("arrayValue", "mapValue"):
                return self.translateVal(dataType, dict_[dataType])
            elif dataType in ("arrayValue"):
                lst = []
                for entry in dict_["arrayValue"]["values"]:
                    lst.append(self.translateDict(entry))
                retDict[dataType] = lst
            elif dataType in ("mapValue"):
                map = {}
                for field in dict_["mapValue"]["fields"]:
                    map[field] = self.translateDict2(
                        dict_["mapValue"]["fields"][field])
                return map
        return retDict

    def translateDict(self, dict_):
        # see https://cloud.google.com/functions/docs/calling/cloud-firestore
        retDict = {}
        if len(dict_) > 0:
            for k in dict_:
                # print("translateDict. k={}, type={}".format(k, type(dict_[k])))
                for k2 in dict_[k]:
                    dataType = k2
                if dataType not in ("arrayValue", "mapValue"):
                    retDict[k] = self.translateVal(k, dict_[k])
                elif dataType in ("arrayValue"):
                    lst = []
                    for entry in dict_[k]["arrayValue"]["values"]:
                        lst.append(self.translateDict2(entry))
                    retDict[k] = lst
                elif dataType in ("mapValue"):
                    map = {}
                    for field in dict_[k]["mapValue"]["fields"]:
                        map[field] = self.translateDict2(
                            dict_[k]["mapValue"]["fields"][field]
                        )
                    retDict[k] = map
        return retDict

    def change_or_removed(self, old_dict, new_dict):
        removed_keys = []
        changed_keys = []
        added_keys = []
        for k in old_dict:
            if k not in new_dict:
                removed_keys.append({k: old_dict[k]})
            else:
                if old_dict[k] != new_dict[k]:
                    changed_keys.append(
                        {k: {"oldVal": old_dict[k], "val": new_dict[k]}}
                    )

        for k in new_dict:
            if k not in old_dict:
                added_keys.append({k: new_dict[k]})

        return removed_keys, changed_keys, added_keys
