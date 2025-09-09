import pytest
from py_load_chembl.utils import topological_sort_tables


def test_topological_sort_happy_path():
    """
    Tests that a valid set of tables is sorted correctly according to dependencies.
    """
    # B depends on A, C depends on B, D depends on A
    # Valid order: A, B, D, C  or A, D, B, C
    tables = ["A", "B", "C", "D"]
    dependencies = [("B", "A"), ("C", "B"), ("D", "A")]

    sorted_tables = topological_sort_tables(tables, dependencies)

    # Check that all tables are present
    assert len(sorted_tables) == 4
    assert set(sorted_tables) == set(tables)

    # Check for valid ordering
    assert sorted_tables.index("A") < sorted_tables.index("B")
    assert sorted_tables.index("B") < sorted_tables.index("C")
    assert sorted_tables.index("A") < sorted_tables.index("D")


def test_topological_sort_no_dependencies():
    """
    Tests that a list of tables with no dependencies between them
    is returned, though the order is not guaranteed.
    """
    tables = ["A", "B", "C"]
    dependencies = [("X", "Y")]  # No relevant dependencies

    sorted_tables = topological_sort_tables(tables, dependencies)
    assert len(sorted_tables) == 3
    assert set(sorted_tables) == set(tables)


def test_topological_sort_complex_graph():
    """
    Tests a more complex, multi-level dependency graph.
    """
    # F -> B -> A
    # F -> C -> A
    # F -> D -> B
    # E -> C
    # Expected order: A must be before B,C. B,C must be before D,E,F.
    tables = ["A", "B", "C", "D", "E", "F"]
    dependencies = [
        ("F", "B"), ("F", "C"), ("F", "D"),
        ("B", "A"),
        ("C", "A"),
        ("D", "B"),
        ("E", "C"),
    ]
    sorted_tables = topological_sort_tables(tables, dependencies)
    assert sorted_tables.index("A") < sorted_tables.index("B")
    assert sorted_tables.index("A") < sorted_tables.index("C")
    assert sorted_tables.index("B") < sorted_tables.index("D")
    assert sorted_tables.index("B") < sorted_tables.index("F")
    assert sorted_tables.index("C") < sorted_tables.index("E")
    assert sorted_tables.index("C") < sorted_tables.index("F")


def test_topological_sort_cycle_detection():
    """
    Tests that a ValueError is raised if a cyclic dependency is detected.
    """
    tables = ["A", "B", "C"]
    dependencies = [("A", "B"), ("B", "C"), ("C", "A")]  # Cycle A -> B -> C -> A

    with pytest.raises(ValueError) as excinfo:
        topological_sort_tables(tables, dependencies)
    assert "A cyclic dependency was detected" in str(excinfo.value)
    assert "A, B, C" in str(excinfo.value)

def test_topological_sort_with_unrelated_tables():
    """
    Tests that tables not involved in the dependency graph are still present.
    """
    tables = ["A", "B", "C", "X", "Y"]
    dependencies = [("B", "A"), ("C", "B")]

    sorted_tables = topological_sort_tables(tables, dependencies)
    assert len(sorted_tables) == 5
    assert set(sorted_tables) == {"A", "B", "C", "X", "Y"}
    assert sorted_tables.index("A") < sorted_tables.index("B")
    assert sorted_tables.index("B") < sorted_tables.index("C")
