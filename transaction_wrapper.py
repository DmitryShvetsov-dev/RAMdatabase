from interfaces import DataBaseAbstractClass


class WrappedDatabase(DataBaseAbstractClass):
    def __init__(self, database: DataBaseAbstractClass):
        self.database = database
        self.layers = []

    def set(self, k: str, v: str) -> None:
        if len(self.layers) == 0:
            self.database.set(k, v)
        else:
            self.layers[-1][k] = v

        return None

    def get(self, k: str) -> str:
        for item in self.layers[::-1]:
            for key, v in item.items():
                if key == k:
                    return v
        else:
            return self.database.get(k)

    def unset(self, k: str) -> None:
        if len(self.layers) >= 1:
            self.layers[-1][k] = "NULL"
            return None
        else:
            return self.database.unset(k)

    def counts(self, v: str) -> int:
        _ = 0
        seen_names = set()
        if len(self.layers) >= 1:
            for item in self.layers[::-1]:
                for key, val in item.items():
                    if key not in seen_names and val == v:
                        _ += 1
                    seen_names.add(key)
            for key, val in self.database.read_database().items():
                if key not in seen_names and val == v:
                    _ += 1
                    seen_names.add(key)
            return _
        else:
            return self.database.counts(v)

    def find(self, v: str) -> str:
        str_ = ""
        seen_names = set()
        if len(self.layers) >= 1:
            for item in self.layers[::-1]:
                for key, val in item.items():
                    if key not in seen_names and val == v:
                        str_ += f"{key} "
                    seen_names.add(key)
            for key, val in self.database.read_database().items():
                if key not in seen_names and val == v:
                    str_ += f"{key} "
                    seen_names.add(key)
            print(seen_names)
            return str_[:-1]
        else:
            return self.database.find(v)

    def begin(self) -> None:
        self.layers.append({})
        return None

    def rollback(self) -> None:
        if len(self.layers) == 1:
            self.layers.clear()
        elif len(self.layers) >= 2:
            self.layers.pop()
        return None

    def commit(self) -> None:
        if len(self.layers) == 1:
            self.database.commit(self.layers[0])
            self.layers.clear()
        elif len(self.layers) >= 2:
            self.layers[-2].update(self.layers[-1])
            self.layers.pop()
        else:
            return None
