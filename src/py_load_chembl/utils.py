from collections import defaultdict, deque
from typing import List, Tuple


def topological_sort_tables(
    tables_to_sort: List[str], dependencies: List[Tuple[str, str]]
) -> List[str]:
    """
    Performs a topological sort on a list of tables based on their dependencies.

    Args:
        tables_to_sort: A list of table names that need to be sorted.
        dependencies: A list of tuples, where each tuple (u, v) means that
                      table u depends on table v (u has a FK to v).

    Returns:
        A list of table names in a valid loading order.

    Raises:
        ValueError: If a cycle is detected in the dependencies, meaning they
                    cannot be sorted.
    """
    # Only consider dependencies that involve the tables we actually need to sort
    relevant_dependencies = [
        (u, v) for u, v in dependencies if u in tables_to_sort and v in tables_to_sort
    ]

    in_degree = {table: 0 for table in tables_to_sort}
    adj = defaultdict(list)

    for u, v in relevant_dependencies:
        # u depends on v, so an edge goes from v to u
        adj[v].append(u)
        in_degree[u] += 1

    # Queue for all nodes with in-degree 0
    queue = deque([table for table in tables_to_sort if in_degree[table] == 0])
    sorted_order = []

    while queue:
        table = queue.popleft()
        sorted_order.append(table)

        for neighbor in adj[table]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    if len(sorted_order) != len(tables_to_sort):
        # Find the tables involved in the cycle for a better error message
        cyclic_tables = sorted([table for table in tables_to_sort if table not in sorted_order])
        raise ValueError(
            "A cyclic dependency was detected among the tables. "
            f"Cannot determine a valid loading order. "
            f"Tables involved in the cycle: {', '.join(cyclic_tables)}"
        )

    return sorted_order
