from database import Database


def process_command(db: Database, command: str) -> str | None:
    """Обработает одну команду пользователя и вернёт результат."""
    parts = command.strip().split()
    if not parts:
        return None
    cmd = parts[0].upper()
    args = parts[1:]

    match cmd, args:
        case "SET", [key, value]:
            db.set_value(key, value)
            return None
        case "GET", [key]:
            return db.get_value(key)
        case "UNSET", [key]:
            db.unset_value(key)
            return None
        case "COUNTS", [value]:
            return str(db.count_value(value))
        case "FIND", [value]:
            keys = db.find_keys(value)
            return " ".join(sorted(keys)) if keys else ""
        case "BEGIN", []:
            db.begin_transaction()
            return None
        case "ROLLBACK", []:
            return None if db.rollback_transaction() else "NO TRANSACTION"
        case "COMMIT", []:
            return None if db.commit_transaction() else "NO TRANSACTION"
        case "END", []:
            return "EXIT"
        case _:
            return f"UNKNOWN COMMAND: {cmd}"
