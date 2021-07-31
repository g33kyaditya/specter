from pymongo import MongoClient


class Job(object):
    def __init__(self):
        pass

    def run(self, data):
        print(f"Base run called!")


class ValidateJob(Job):
    def __init__(self):
        pass

    def run(self, data):
        # Name and symbol are extremely important
        status, name, errors, warnings = self._validate(data)
        if not status:
            print(f"Something is wrong with dataset for {name}")
            print(f"The errors are: {errors}")
            return status, data

        if len(warnings) > 0:
            print(f"Data has the following issues: {warnings}")

        return status, data

    def _validate(self, data):
        status = True
        errors = []
        warnings = []

        # These are the absolute essential data points we need
        # Not having these, is an error
        name = data.get("name", None)
        if not name:
            status = False
            errors.append(f"Did not find a valid name for {data}")

        symbol = data.get("symbol", None)
        if not symbol:
            status = False
            errors.append(f"Cryptocoin {name} does not have a symbol")

        if not status:
            return status, name, errors, warnings

        # These are data points which aren't errors but we'd like to
        # treat them as warnings
        # This list is business use case dependent. For now, I'm choosing something very trivial
        keys = {
            "cmcRank": {"default": -1},
            "marketPairCount": {"default": -1},
            "circulatingSupply": {"default": -1},
            "isActive": {"default": False},
            "maxSupply": {"default": -1},
        }

        for k in keys.keys():
            if not data.get(k):
                warnings.append(f"Missing '{k}'")
                data[k] = keys[k]["default"]

        return status, name, errors, warnings


class SaveJob(Job):
    def __init__(self):
        pass

    def run(self, data):
        client = MongoClient()
        db = client.cryptocoins
        collection = db.coins
        try:
            id = collection.insert_one(data).inserted_id
            print(f"Added documend with id: {id}")
        except Exception as e:
            print(f"Exception {e} occured")

        return True, data
