class Database:
    """
    База данных ключ-значение в оперативной памяти с поддержкой
    вложенных транзакций.

    Этот класс реализует базовые команды для установки, получения, удаления
    значений, подсчёта количества вхождений значений и поиска ключей по
    значению. Также поддерживаются транзакционные операции.

    Атрибуты:
        data: Основное хранилище данных.
        transaction_stack: Стек транзакций.
    """

    def __init__(self) -> None:
        self.data: dict[str, str] = {}
        self.transaction_stack: list[list[dict[str, str | None]]] = []

    def begin_transaction(self) -> None:
        """Начинает новую транзакцию."""
        self.transaction_stack.append([])

    def rollback_transaction(self) -> bool:
        """Делает роллбэк текущей транзакции."""
        if not self.transaction_stack:
            return False
        self.transaction_stack.pop()
        return True

    def commit_transaction(self) -> bool:
        """Делает коммит всех транзакций."""
        if not self.transaction_stack:
            return False
        current_transaction = self.transaction_stack.pop()
        for operation in current_transaction:
            for key, value in operation.items():
                if value is None:
                    self.data.pop(key, None)
                else:
                    self.data[key] = value
        return True

    def set_value(self, key: str, value: str) -> None:
        """Добавляет запись в базу или в текущую транзакцию."""
        if self.transaction_stack:
            self.transaction_stack[-1].append({key: value})
        else:
            self.data[key] = value

    def get_value(self, key: str) -> str:
        """Получает значение переменной или NULL при отсутствии."""
        for transaction in reversed(self.transaction_stack):
            for operation in reversed(transaction):
                if key in operation:
                    value = operation[key]
                    return "NULL" if value is None else value
        return self.data.get(key, "NULL")

    def unset_value(self, key: str) -> None:
        """Удаляет запись или добавляет операцию в транзакцию."""
        if self.transaction_stack:
            self.transaction_stack[-1].append({key: None})
        else:
            self.data.pop(key, None)

    def _current_state(self) -> dict[str, str]:
        """Возвращает текущее состояние БД
        с учётом всех незакоммиченных транзакций.
        """
        temp_data = self.data.copy()
        for transaction in self.transaction_stack:
            for operation in transaction:
                for key, val in operation.items():
                    if val is None:
                        temp_data.pop(key, None)
                    else:
                        temp_data[key] = val
        return temp_data

    def count_value(self, value: str) -> int:
        """Выводит количество, сколько раз значение встречается в базе."""
        return list(self._current_state().values()).count(value)

    def find_keys(self, value: str) -> set[str]:
        """Выводит все переменные с заданным значением."""
        return {
            key for key, val in self._current_state().items() if val == value
        }
