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
            self.layers[-1].pop(k, None)
            return None
        else:
            return self.database.unset(k)

    def counts(self, v: str) -> int:
        return self.database.counts(v)

    def find(self, v: str) -> str:
        return self.database.find(v)

    def begin(self):
        self.layers.append({})
        print(self.layers)
        return None

    def rollback(self):
        if len(self.layers) == 1:
            self.layers.clear()
        elif len(self.layers) >= 2:
            self.layers.pop()

    def commit(self):
        if len(self.layers) == 1:
            self.database.commit(self.layers[0])
            self.layers.clear()
        elif len(self.layers) >= 2:
            self.layers[-2].update(self.layers[-1])
            self.layers.pop()
        else:
            return None
