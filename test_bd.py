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


# -----------------------
# Поведение / граничные случаи
# -----------------------

def test_case_sensitivity(handler):
    # ключи чувствительны к регистру
    res = run(handler, ["SET a 1", "SET A 2", "GET a", "GET A", "COUNTS 1", "COUNTS 2"])
    assert res == ["1", "2", 1, 1]


def test_set_same_value_no_count_change(handler):
    # присваивание того же значения не должно увеличивать счетчик
    res = run(handler, ["SET A 10", "SET B 10", "COUNTS 10", "SET A 10", "COUNTS 10"])
    assert res == [2, 2]


def test_unset_nonexistent_noop(handler):
    # UNSET несуществующего ключа — безошибочно, без изменений
    res = run(handler, ["UNSET X", "COUNTS 10", "FIND 10", "GET X"])
    assert res == [0, "", "NULL"]


def test_unset_then_set_in_transaction_then_rollback(handler):
    # UNSET в транзакции, потом новое значение во вложенной, откат внутренней,
    # откат внешней — исходное значение восстанавливается
    res = run(
        handler,
        [
            "SET A 10",
            "BEGIN",
            "UNSET A",
            "GET A",
            "BEGIN",
            "SET A 20",
            "GET A",
            "ROLLBACK",
            "GET A",
            "ROLLBACK",
            "GET A",
        ],
    )
    assert res == ["NULL", "20", "NULL", "10"]


def test_set_unset_set_counts_in_nested_transactions(handler):
    # проверяем, как меняется COUNTS при вложенных транзакциях с UNSET/ROLLBACK/COMMIT
    res = run(
        handler,
        [
            "SET A 10",
            "BEGIN",
            "SET A 20",   # A=20 в первой транзакции
            "BEGIN",
            "UNSET A",    # A убирают во вложенной
            "COUNTS 20",  # 0 внутри вложенной
            "ROLLBACK",   # возвращаем A=20
            "COUNTS 20",  # 1 после rollback
            "COMMIT",     # фиксируем A=20
            "COUNTS 20",  # 1 после commit
            "GET A",
        ],
    )
    assert res == [0, 1, 1, "20"]


def test_multiple_unsets_and_sets(handler):
    # многократный UNSET одного ключа — должен быть безопасен,
    # затем повторная установка восстанавливает счетчики
    res = run(
        handler,
        [
            "SET A 10",
            "SET B 10",
            "UNSET A",
            "UNSET A",
            "COUNTS 10",
            "SET A 10",
            "COUNTS 10",
        ],
    )
    assert res == [1, 2]


def test_deep_nested_transactions_counts_restore(handler):
    # глубокая вложенность транзакций и проверка откатов/фиксаций на счетчики
    res = run(
        handler,
        [
            "SET A 1",
            "SET B 2",
            "SET C 1",
            "COUNTS 1",    # 2 (A,C)
            "BEGIN",
            "SET D 1",
            "COUNTS 1",    # 3 (A,C,D)
            "BEGIN",
            "UNSET A",
            "COUNTS 1",    # 2 (C,D)
            "BEGIN",
            "SET C 3",
            "COUNTS 1",    # 1 (D)
            "ROLLBACK",
            "COUNTS 1",    # 2 (C restored, D)
            "ROLLBACK",
            "COUNTS 1",    # 3 (A restored, C, D)
            "COMMIT",
            "COUNTS 1",    # 3 (committed)
        ],
    )
    assert res == [2, 3, 2, 1, 2, 3, 3]


def test_find_after_rollback_for_new_variable(handler):
    # FIND должен отражать текущее видимое состояние (включая незакоммиченные изменения)
    res = run(
        handler,
        [
            "SET A 10",
            "BEGIN",
            "SET B 10",
            "FIND 10",
            "ROLLBACK",
            "FIND 10",
        ],
    )
    # сначала A и B (порядок не гарантирован), потом только A
    assert set(res[0].split()) == {"A", "B"}
    assert res[1] == "A"


def test_commit_inner_changes_visible_in_outer(handler):
    # COMMIT внутренней транзакции должен применить изменения к родительской транзакции
    res = run(
        handler,
        [
            "SET A 10",
            "BEGIN",
            "SET B 10",
            "BEGIN",
            "SET A 20",
            "COMMIT",       # фиксация в родительской
            "COUNTS 10",    # B только (1)
            "GET A",        # 20 (в родительской)
            "COMMIT",       # фиксация в базу
            "GET A",
            "COUNTS 10",
        ],
    )
    assert res == [1, "20", "20", 1]


def test_rollback_removes_new_variable_created_in_transaction(handler):
    res = run(handler, ["BEGIN", "SET A 5", "GET A", "ROLLBACK", "GET A"])
    assert res == ["5", "NULL"]


def test_commit_inner_then_outer_rollback(handler):
    # коммит внутренней транзакции делает изменения видимыми в родительской,
    # а rollback родительской отменяет все эти изменения
    res = run(
        handler,
        [
            "BEGIN",
            "BEGIN",
            "SET A 1",
            "COMMIT",   # A=1 в родителе
            "GET A",    # 1
            "ROLLBACK", # откат родителя -> A исчезнет
            "GET A",
        ],
    )
    assert res == ["1", "NULL"]


def test_nested_commit_and_rollback_sequence(handler):
    # сложная последовательность commit/rollback
    res = run(
        handler,
        [
            "SET A 1",
            "BEGIN",
            "SET A 2",
            "BEGIN",
            "SET A 3",
            "ROLLBACK",   # вернулись к 2
            "GET A",
            "BEGIN",
            "SET A 4",
            "COMMIT",     # фиксируем 4 в родительской
            "GET A",
            "ROLLBACK",   # откат к значениям до первой BEGIN (1)
            "GET A",
        ],
    )
    assert res == ["2", "4", "1"]


def test_set_same_value_multiple_times_no_dupe_counts(handler):
    # Если ключ несколько раз присваивается одному и тому же значению,
    # COUNTS не должен считать это за несколько ключей
    res = run(
        handler,
        [
            "SET A 10",
            "SET A 10",
            "SET B 10",
            "COUNTS 10",
        ],
    )
    assert res == [2]


def test_key_name_special_chars(handler):
    # ключи могут содержать дефисы/подчеркивания и другие не-пробельные символы
    res = run(
        handler,
        [
            "SET key-1 v",
            "SET key_1 v",
            "GET key-1",
            "GET key_1",
            "FIND v",
        ],
    )
    assert res[0] == "v"
    assert res[1] == "v"
    assert set(res[2].split()) == {"key-1", "key_1"}


def test_many_nested_begin_rollback(handler):
    # стресс: много уровней вложенности BEGIN -> ROLLBACK всё откатывает корректно
    lines = ["SET A base"]
    # открыть 30 транзакций, в каждой установить уникальный ключ
    for i in range(30):
        lines.append("BEGIN")
        lines.append(f"SET K{i} V{i}")
        lines.append(f"GET K{i}")
    # откатать все 30 раз — после каждого ROLLBACK ключ должен исчезать
    for i in reversed(range(30)):
        lines.append("ROLLBACK")
        lines.append(f"GET K{i}")
    # в конце базовое значение A должно остаться
    lines.append("GET A")

    res = run(handler, lines)
    # Проверяем несколько точек: первые 30 GET дают значения V0..V29,
    # затем после откатов GET K* -> NULL, последний GET A -> "base"
    assert res[0:30] == [f"V{i}" for i in range(30)]
    # после rollback'ов проверки на NULL
    assert res[30:60] == ["NULL"] * 30
    assert res[-1] == "base"


def test_unset_then_set_again_in_same_transaction(handler):
    # UNSET и затем SET того же ключа в одной транзакции — должно работать,
    # откат этой транзакции восстановит исходное значение
    res = run(
        handler,
        [
            "SET A 1",
            "BEGIN",
            "UNSET A",
            "GET A",
            "SET A 2",
            "GET A",
            "ROLLBACK",
            "GET A",
        ],
    )
    assert res == ["NULL", "2", "1"]


def test_find_empty_and_counts_zero(handler):
    # пустой FIND должен вернуть пустую строку, COUNTS для несуществующего значения 0
    res = run(handler, ["FIND 999", "COUNTS 999"])
    assert res == ["", 0]


def test_multiple_commits_in_row(handler):
    # commit внутри транзакций и стек коммитов должен работать корректно
    res = run(
        handler,
        [
            "BEGIN",
            "SET A 10",
            "BEGIN",
            "SET B 20",
            "COMMIT",  # фиксируем B в родителе
            "GET B",
            "COMMIT",  # фиксируем родителя (A,B)
            "GET A",
            "GET B",
            "COMMIT",  # лишний commit — без вывода / ошибок
        ],
    )
    assert res == ["20", "10", "20"]


def test_reassignment_back_to_previous_value_changes_counts_correctly(handler):
    # A меняют 10->20->10 — в конце counts для 10=1
    res = run(
        handler,
        [
            "SET A 10",
            "SET B 20",
            "SET A 20",
            "SET A 10",
            "COUNTS 10",
            "COUNTS 20",
        ],
    )
    assert res == [1, 1]


def test_find_after_unset_in_outer_then_inner_set_and_commit(handler):
    # Внешняя транзакция делает UNSET, во внутренней задают новое значение и коммитят:
    # после коммита внутренней — значение должно существовать в родительской
    res = run(
        handler,
        [
            "SET A 1",
            "BEGIN",
            "UNSET A",
            "BEGIN",
            "SET A 2",
            "COMMIT",   # теперь в родителе A=2
            "FIND 2",
            "ROLLBACK",
            "GET A",
        ],
    )
    # FIND 2 должно вернуть A, после final ROLLBACK A должен вернуться к 1 (т.к. внешняя транзакция откатила UNSET->SET)
    assert set(res[0].split()) == {"A"}
    assert res[1] == "1"
