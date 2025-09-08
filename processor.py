from interfaces import DataBaseAbstractClass


class CommandHandler:
    def __init__(self, database: DataBaseAbstractClass):
        self.database = database

    def execute(self, command: str, args: list[str]) -> str | int | None:
        command = command.upper()
        if command == "SET":
            return self.__handle_set(args)
        elif command == "GET":
            return self.__handle_get(args)
        elif command == "UNSET":
            return self.__handle_unset(args)
        elif command == "COUNTS":
            return self.__handle_counts(args)
        elif command == "FIND":
            return self.__handle_find(args)
        elif command == "HELP":
            return self.__handle_help()
        elif command == "BEGIN":
            return self.__handle_begin()
        elif command == "ROLLBACK":
            return self.__handle_rollback()
        elif command == "COMMIT":
            return self.__handle_commit()
        elif command == "END":
            return "END"
        else:
            return "Ошибка в команде. Введите HELP для справки."

    def __handle_set(self, args: list[str]) -> str | None:
        if len(args) != 2:
            return "SET требует 2 аргумента."
        k, v = args
        self.database.set(k, v)
        return None

    def __handle_get(self, args: list[str]) -> str:
        if len(args) != 1:
            return "GET требует 1 аргумент."
        k = args[0]
        return self.database.get(k)

    def __handle_unset(self, args: list[str]) -> str | None:
        if len(args) != 1:
            return "UNSET требует 1 аргумент."
        k = args[0]
        return self.database.unset(k)

    def __handle_counts(self, args: list[str]) -> int | str:
        if len(args) != 1:
            return "COUNTS требует 1 аргумент."
        k = args[0]
        return self.database.counts(k)

    def __handle_find(self, args: list[str]) -> str:
        if len(args) != 1:
            return "FIND требует 1 аргумент."
        k = args[0]
        return self.database.find(k)

    def __handle_help(self) -> str:
        return "Команды:\nSET - сохраняет аргумент в базе данных (формат SET name value).\nGET - возвращает, ранее сохраненную переменную (формат GET name). Если такой переменной не было сохранено, возвращает NULL.\nUNSET - удаляет, ранее установленную переменную (формат UNSET name). Если значение не было установлено, не делает ничего.\nCOUNTS - показывает сколько раз данные значение встречается в базе данных (формат COUNTS name).\nFIND - выводит найденные установленные переменные для данного значения (Формат FIND name).\nEND - закрывает приложение.\nBEGIN - начало транзакции.\nROLLBACK - откат текущей (самой внутренней) транзакции.\nCOMMIT - фиксация изменений текущей (самой внутренней) транзакции."

    def __handle_begin(self):
        return self.database.begin()

    def __handle_rollback(self):
        return self.database.rollback()

    def __handle_commit(self):
        return self.database.commit()
