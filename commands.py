from abc import ABC, abstractmethod

from database import Database


class Command(ABC):
    """Абстрактный базовый класс для всех команд."""

    @abstractmethod
    def execute(self, db: Database, args: list[str]) -> str | None:
        """Выполняет команду с указанными аргументами."""
        pass

    @abstractmethod
    def validate_args(self, args: list[str]) -> bool:
        """Проверяет корректность аргументов команды."""
        pass


class SetCommand(Command):
    def validate_args(self, args: list[str]) -> bool:
        return len(args) == 2

    def execute(self, db: Database, args: list[str]) -> str | None:
        if not self.validate_args(args):
            return "ERROR: SET requires KEY VALUE"
        key, value = args
        db.set_value(key, value)
        return None


class GetCommand(Command):
    def validate_args(self, args: list[str]) -> bool:
        return len(args) == 1

    def execute(self, db: Database, args: list[str]) -> str | None:
        if not self.validate_args(args):
            return "ERROR: GET requires KEY"
        return db.get_value(args[0])


class UnsetCommand(Command):
    def validate_args(self, args: list[str]) -> bool:
        return len(args) == 1

    def execute(self, db: Database, args: list[str]) -> str | None:
        if not self.validate_args(args):
            return "ERROR: UNSET requires KEY"
        db.unset_value(args[0])
        return None


class CountsCommand(Command):
    def validate_args(self, args: list[str]) -> bool:
        return len(args) == 1

    def execute(self, db: Database, args: list[str]) -> str | None:
        if not self.validate_args(args):
            return "ERROR: COUNTS requires VALUE"
        return str(db.count_value(args[0]))


class FindCommand(Command):
    def validate_args(self, args: list[str]) -> bool:
        return len(args) == 1

    def execute(self, db: Database, args: list[str]) -> str | None:
        if not self.validate_args(args):
            return "ERROR: FIND requires VALUE"
        keys = db.find_keys(args[0])
        return " ".join(sorted(keys)) if keys else ""


class BeginCommand(Command):
    def validate_args(self, args: list[str]) -> bool:
        return len(args) == 0

    def execute(self, db: Database, args: list[str]) -> str | None:
        if not self.validate_args(args):
            return "ERROR: BEGIN takes no arguments"
        db.begin_transaction()
        return None


class RollbackCommand(Command):
    def validate_args(self, args: list[str]) -> bool:
        return len(args) == 0

    def execute(self, db: Database, args: list[str]) -> str | None:
        if not self.validate_args(args):
            return "ERROR: ROLLBACK takes no arguments"
        return None if db.rollback_transaction() else "NO TRANSACTION"


class CommitCommand(Command):
    def validate_args(self, args: list[str]) -> bool:
        return len(args) == 0

    def execute(self, db: Database, args: list[str]) -> str | None:
        if not self.validate_args(args):
            return "ERROR: COMMIT takes no arguments"
        return None if db.commit_transaction() else "NO TRANSACTION"


class EndCommand(Command):
    def validate_args(self, args: list[str]) -> bool:
        return len(args) == 0

    def execute(self, db: Database, args: list[str]) -> str | None:
        if not self.validate_args(args):
            return "ERROR: END takes no arguments"
        return "EXIT"


class UnknownCommand(Command):
    def __init__(self, name: str):
        self.name = name

    def validate_args(self, args: list[str]) -> bool:
        return True

    def execute(self, db: Database, args: list[str]) -> str | None:
        return f"UNKNOWN COMMAND: {self.name}"


class CommandFactory:
    """Фабрика для создания команд по их имени."""

    @staticmethod
    def create_command(command_name: str) -> Command:
        command_map = {
            "SET": SetCommand(),
            "GET": GetCommand(),
            "UNSET": UnsetCommand(),
            "COUNTS": CountsCommand(),
            "FIND": FindCommand(),
            "BEGIN": BeginCommand(),
            "ROLLBACK": RollbackCommand(),
            "COMMIT": CommitCommand(),
            "END": EndCommand(),
        }
        return command_map.get(
            command_name.upper(), UnknownCommand(command_name)
        )


def process_command(db: Database, command_str: str) -> str | None:
    """Обрабатывает команду пользователя."""
    parts = command_str.strip().split()
    if not parts:
        return None

    command_name = parts[0]
    args = parts[1:] if len(parts) > 1 else []

    command = CommandFactory.create_command(command_name)
    return command.execute(db, args)
