import pytest

from database import Database
from commands import process_command


@pytest.fixture
def db() -> Database:
    """Фикстура, создающая экземпляр базы данных."""
    return Database()


@pytest.fixture
def db_with_data(db: Database):
    """Фикстура, создающая базу данных с предустановленными значениями."""
    db.set_value("A", "10")
    db.set_value("B", "20")
    db.set_value("C", "10")
    return db


class TestDatabase:
    """Тесты для класса Database."""

    def test_set_get_value(self, db: Database):
        """Тестирование базовых операций установки и получения значений."""
        db.set_value("A", "10")
        assert db.get_value("A") == "10"
        assert db.get_value("B") == "NULL"

    def test_unset_value(self, db: Database):
        """Тестирование удаления значений."""
        db.set_value("A", "10")
        assert db.get_value("A") == "10"
        db.unset_value("A")
        assert db.get_value("A") == "NULL"
        db.unset_value("B")
        assert db.get_value("B") == "NULL"

    def test_count_value(self, db: Database):
        """Тестирование подсчета количества вхождений значений."""
        db.set_value("A", "10")
        assert db.count_value("10") == 1
        db.set_value("B", "10")
        assert db.count_value("10") == 2
        db.set_value("C", "20")
        assert db.count_value("10") == 2
        assert db.count_value("20") == 1
        assert db.count_value("30") == 0

    def test_find_keys(self, db_with_data: Database):
        """Тестирование поиска ключей по значению."""
        assert db_with_data.find_keys("10") == {"A", "C"}
        assert db_with_data.find_keys("20") == {"B"}
        assert db_with_data.find_keys("30") == set()

    def test_begin_transaction(self, db: Database):
        """Тестирование начала транзакции."""
        db.begin_transaction()
        assert len(db.transaction_stack) == 1
        db.begin_transaction()
        assert len(db.transaction_stack) == 2

    def test_rollback_transaction_empty(self, db: Database):
        """Тестирование отката пустой транзакции."""
        assert not db.rollback_transaction()

    def test_rollback_transaction(self, db: Database):
        """Тестирование отката транзакции."""
        db.set_value("A", "10")
        db.begin_transaction()
        db.set_value("A", "20")
        assert db.get_value("A") == "20"
        assert db.rollback_transaction()
        assert db.get_value("A") == "10"

    def test_commit_transaction_empty(self, db: Database):
        """Тестирование фиксации пустой транзакции."""
        assert not db.commit_transaction()

    def test_commit_transaction(self, db: Database):
        """Тестирование фиксации транзакции."""
        db.begin_transaction()
        db.set_value("A", "10")
        db.begin_transaction()
        db.set_value("B", "20")
        assert db.commit_transaction()
        assert db.get_value("A") == "10"
        assert db.get_value("B") == "20"
        assert len(db.transaction_stack) == 1
        db.rollback_transaction()
        assert db.get_value("A") == "NULL"
        assert db.get_value("B") == "NULL"

    def test_nested_transactions(self, db: Database):
        """Тестирование вложенных транзакций."""
        db.begin_transaction()
        db.set_value("A", "10")
        db.begin_transaction()
        db.set_value("A", "20")
        db.begin_transaction()
        db.set_value("A", "30")
        assert db.get_value("A") == "30"
        assert db.rollback_transaction()
        assert db.get_value("A") == "20"
        assert db.commit_transaction()
        assert db.get_value("A") == "20"
        assert len(db.transaction_stack) == 1
        assert db.commit_transaction()
        assert db.get_value("A") == "20"
        assert len(db.transaction_stack) == 0

    def test_transaction_with_unset(self, db: Database):
        """Тестирование операции unset в транзакции."""
        db.set_value("A", "10")
        db.begin_transaction()
        db.unset_value("A")
        assert db.get_value("A") == "NULL"
        assert db.rollback_transaction()
        assert db.get_value("A") == "10"

    def test_current_state(self, db: Database):
        """Тестирование получения текущего состояния базы данных."""
        db.set_value("A", "10")
        db.set_value("B", "20")
        assert db._current_state() == {"A": "10", "B": "20"}
        db.begin_transaction()
        db.set_value("A", "30")
        db.set_value("C", "40")
        assert db._current_state() == {"A": "30", "B": "20", "C": "40"}
        db.unset_value("B")
        assert db._current_state() == {"A": "30", "C": "40"}

    def test_commit_nested_transactions(self, db: Database):
        """Проверка того, что COMMIT фиксирует только внутреннюю транзакцию."""
        db.begin_transaction()
        db.set_value("A", "10")
        db.begin_transaction()
        db.set_value("B", "20")
        db.begin_transaction()
        db.set_value("C", "30")
        assert db.commit_transaction()
        assert len(db.transaction_stack) == 2
        assert db.get_value("A") == "10"
        assert db.get_value("B") == "20"
        assert db.get_value("C") == "30"
        assert db.rollback_transaction()
        assert db.get_value("A") == "10"
        assert db.get_value("B") == "NULL"
        assert db.get_value("C") == "NULL"
        assert db.commit_transaction()
        assert db.get_value("A") == "10"
        assert len(db.transaction_stack) == 0


class TestIntegration:
    """Интеграционные тесты для проверки полных сценариев использования."""

    def test_scenario_1(self, db: Database):
        """Тестирование сценария из задания."""
        assert process_command(db, "GET A") == "NULL"
        process_command(db, "SET A 10")
        assert process_command(db, "GET A") == "10"
        assert process_command(db, "COUNTS 10") == "1"
        process_command(db, "SET B 20")
        process_command(db, "SET C 10")
        assert process_command(db, "COUNTS 10") == "2"
        process_command(db, "UNSET B")
        assert process_command(db, "GET B") == "NULL"

    def test_transactions_scenario(self, db: Database):
        """Тестирование сценария с транзакциями из задания."""
        process_command(db, "BEGIN")
        process_command(db, "SET A 10")
        process_command(db, "BEGIN")
        process_command(db, "SET A 20")
        process_command(db, "BEGIN")
        process_command(db, "SET A 30")
        assert process_command(db, "GET A") == "30"
        process_command(db, "ROLLBACK")
        assert process_command(db, "GET A") == "20"
        process_command(db, "COMMIT")
        assert process_command(db, "GET A") == "20"
        assert len(db.transaction_stack) == 1

    def test_transaction_isolation(self, db: Database):
        """Тестирование изоляции транзакций."""
        process_command(db, "SET A 10")
        process_command(db, "BEGIN")
        process_command(db, "SET A 20")
        process_command(db, "SET B 30")
        process_command(db, "ROLLBACK")
        assert process_command(db, "GET A") == "10"
        assert process_command(db, "GET B") == "NULL"

    def test_mixed_operations_with_inner_commit(self, db: Database):
        """Тестирование операций с фиксацией внутренней транзакции."""
        process_command(db, "SET A 10")
        process_command(db, "BEGIN")
        process_command(db, "SET B 20")
        process_command(db, "BEGIN")
        process_command(db, "SET A 30")
        process_command(db, "SET C 40")
        assert process_command(db, "GET A") == "30"
        assert process_command(db, "GET B") == "20"
        assert process_command(db, "GET C") == "40"
        process_command(db, "COMMIT")
        assert len(db.transaction_stack) == 1
        assert process_command(db, "GET A") == "30"
        assert process_command(db, "GET B") == "20"
        assert process_command(db, "GET C") == "40"
        process_command(db, "ROLLBACK")
        assert process_command(db, "GET A") == "10"
        assert process_command(db, "GET B") == "NULL"
        assert process_command(db, "GET C") == "NULL"


class TestEdgeCases:
    """Тесты для проверки граничных случаев и обработки ошибок."""

    def test_empty_database(self, db: Database):
        """Тестирование операций с пустой базой данных."""
        assert process_command(db, "GET A") == "NULL"
        assert process_command(db, "COUNTS 10") == "0"
        assert process_command(db, "FIND 10") == ""
        assert process_command(db, "UNSET A") is None

    def test_multiple_rollbacks(self, db: Database):
        """Тестирование многократных откатов транзакций."""
        process_command(db, "BEGIN")
        process_command(db, "SET A 10")
        process_command(db, "BEGIN")
        process_command(db, "SET A 20")
        assert process_command(db, "ROLLBACK") is None
        assert process_command(db, "GET A") == "10"
        assert process_command(db, "ROLLBACK") is None
        assert process_command(db, "GET A") == "NULL"
        assert process_command(db, "ROLLBACK") == "NO TRANSACTION"

    def test_commit_inner_transaction_visibility(self, db: Database):
        """Тестирование изменений после коммита внутренней транзакции."""
        process_command(db, "BEGIN")
        process_command(db, "SET A 10")
        process_command(db, "BEGIN")
        process_command(db, "SET B 20")
        process_command(db, "BEGIN")
        process_command(db, "SET C 30")
        process_command(db, "COMMIT")
        assert len(db.transaction_stack) == 2
        assert process_command(db, "GET A") == "10"
        assert process_command(db, "GET B") == "20"
        assert process_command(db, "GET C") == "30"
        process_command(db, "COMMIT")
        assert len(db.transaction_stack) == 1
        assert process_command(db, "GET A") == "10"
        assert process_command(db, "GET B") == "20"
        assert process_command(db, "GET C") == "30"
        process_command(db, "ROLLBACK")
        assert process_command(db, "GET A") == "NULL"
        assert process_command(db, "GET B") == "NULL"
        assert process_command(db, "GET C") == "NULL"

    def test_transaction_isolation_unset(self, db: Database):
        """Тестирование изоляции при удалении значений."""
        process_command(db, "SET A 10")
        process_command(db, "BEGIN")
        process_command(db, "UNSET A")
        assert process_command(db, "GET A") == "NULL"
        process_command(db, "ROLLBACK")
        assert process_command(db, "GET A") == "10"
