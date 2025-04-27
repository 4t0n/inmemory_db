from commands import process_command
from database import Database


def main():
    """Запускает цикл ожидания команд пользователя."""
    db = Database()
    while True:
        try:
            print("> ", end="", flush=True)
            line = input()
            result = process_command(db, line)
            if result == "EXIT":
                break
            if result is not None:
                print(result)
        except (EOFError, KeyboardInterrupt):
            break


if __name__ == "__main__":
    main()
