from database import RAMDatabase
from processor import CommandHandler
from transaction_wrapper import WrappedDatabase


def main():
    database = RAMDatabase()
    wrapped_database = WrappedDatabase(database)
    processor = CommandHandler(wrapped_database)

    print("Добро пожаловать. Введите HELP для справки.")
    while True:
        try:
            user_input = input("> ")
        except EOFError:
            print("EOF. Работа завершена, бд очищена.")
            break
        user_input_split = user_input.split()
        command = user_input_split[0]
        args = user_input_split[1:]
        result = processor.execute(command, args)

        if result == "END":
            print("Работа завершена, бд очищена.")
            break

        if result != None:
            print(result)


if __name__ == "__main__":
    main()
