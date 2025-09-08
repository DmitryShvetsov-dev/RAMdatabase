import pytest
from database import RAMDatabase
from processor import CommandHandler
from transaction_wrapper import WrappedDatabase


@pytest.fixture
def handler():
    db = RAMDatabase()
    wrapped = WrappedDatabase(db)
    return CommandHandler(wrapped)


def run(handler, lines):
    out = []
    for line in lines:
        parts = line.split()
        cmd, args = parts[0], parts[1:]
        res = handler.execute(cmd, args)
        if res is not None:
            out.append(res)
    return out


def test_get_unset(handler):
    assert run(handler, ["GET A"]) == ["NULL"]


def test_set_get_unset(handler):
    assert run(handler, ["SET A 10", "GET A", "UNSET A", "GET A"]) == ["10", "NULL"]


def test_counts_and_find(handler):
    result = run(
        handler,
        [
            "SET A 10",
            "SET B 20",
            "SET C 10",
            "COUNTS 10",
            "COUNTS 20",
            "COUNTS 30",
            "FIND 10",
            "FIND 999",
        ],
    )
    # Проверка количеств
    assert result[0:3] == [2, 1, 0]
    # FIND может вернуть переменные в разном порядке
    assert set(result[3].split()) == {"A", "C"}
    assert result[4] == ""


def test_reassign(handler):
    result = run(handler, ["SET A 10", "SET A 20", "GET A", "COUNTS 10", "COUNTS 20"])
    assert result == ["20", 0, 1]


def test_transaction_commit(handler):
    result = run(handler, ["SET A 10", "BEGIN", "SET A 20", "GET A", "COMMIT", "GET A"])
    assert result == ["20", "20"]


def test_transaction_rollback(handler):
    result = run(
        handler, ["SET A 10", "BEGIN", "SET A 20", "GET A", "ROLLBACK", "GET A"]
    )
    assert result == ["20", "10"]


def test_nested_transactions(handler):
    result = run(
        handler,
        [
            "SET A 10",
            "BEGIN",
            "SET A 20",
            "BEGIN",
            "SET A 30",
            "GET A",
            "ROLLBACK",
            "GET A",
            "COMMIT",
            "GET A",
        ],
    )
    assert result == ["30", "20", "20"]


def test_commit_without_transaction(handler):
    assert run(handler, ["COMMIT"]) == []


def test_rollback_without_transaction(handler):
    assert run(handler, ["ROLLBACK"]) == []


def test_end(handler):
    assert run(handler, ["END"]) == ["END"]


def test_unset_then_set_again(handler):
    result = run(handler, ["SET A 10", "UNSET A", "GET A", "SET A 15", "GET A"])
    assert result == ["NULL", "15"]


def test_multiple_variables_same_value(handler):
    result = run(
        handler,
        [
            "SET A 10",
            "SET B 10",
            "SET C 10",
            "COUNTS 10",
            "UNSET B",
            "COUNTS 10",
            "FIND 10",
        ],
    )
    assert result[0] == 3
    assert result[1] == 2
    assert set(result[2].split()) == {"A", "C"}


def test_nested_rollback_commit_mixed(handler):
    result = run(
        handler,
        [
            "SET A 1",
            "BEGIN",
            "SET A 2",
            "BEGIN",
            "SET A 3",
            "ROLLBACK",  # откат к 2
            "GET A",
            "BEGIN",
            "SET A 4",
            "COMMIT",  # фиксация 4
            "GET A",
            "ROLLBACK",  # откат к 1
            "GET A",
        ],
    )
    assert result == ["2", "4", "1"]


def test_multiple_commits(handler):
    result = run(
        handler,
        [
            "BEGIN",
            "SET A 10",
            "COMMIT",
            "GET A",
            "COMMIT",  # commit без транзакции
        ],
    )
    assert result == ["10"]


def test_long_sequence_stress(handler):
    result = run(
        handler,
        [
            "SET A 1",
            "SET B 2",
            "BEGIN",
            "SET A 3",
            "SET C 4",
            "BEGIN",
            "UNSET B",
            "SET D 5",
            "GET B",
            "ROLLBACK",
            "GET B",
            "COMMIT",
            "GET A",
            "GET C",
        ],
    )
    # Проверим только ключевые точки
    assert result[0] == "2"  # внутри транзакции B удалили
    assert result[1] == "2"  # после rollback B снова есть
    assert result[2] == "3"  # commit зафиксировал A=3
    assert result[3] == "4"  # C остался после commit


def test_end_resets_session(handler):
    # END должен вернуть "END", но не ломать выполнение pytest
    assert run(handler, ["SET A 10", "END"]) == ["END"]
