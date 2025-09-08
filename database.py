from interfaces import DataBaseAbstractClass


class RAMDatabase(DataBaseAbstractClass):
    def __init__(self):
        self.__database = {}

    def read_database(self) -> dict[str, str]:
        return self.__database.copy()

    def set(self, k: str, v: str) -> None:
        self.__database[k] = v
        return None

    def get(self, k: str) -> str:
        return self.__database.get(k, "NULL")

    def unset(self, k: str) -> None:
        self.__database.pop(k, None)
        return None

    def counts(self, v: str) -> int:
        _ = 0
        for value in self.__database.values():
            if value == v:
                _ += 1
        return _

    def find(self, v: str) -> str:
        str_ = ""
        for key, value in self.__database.items():
            if value == v:
                str_ += f"{key} "
        return str_[:-1]

    def commit(self, new: list):
        self.__database.update(new)
        self.__database = {k: v for k, v in self.__database.items() if v != "NULL"}
