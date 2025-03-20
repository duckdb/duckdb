"""
Optional SQL syntax highlighting for DuckDB Python API using pygments.
"""

try:
    from pygments import highlight
    from pygments.lexers import SqlLexer
    from pygments.formatters import Terminal256Formatter
    HAS_PYGMENTS = True
except ImportError:
    HAS_PYGMENTS = False

from . import DuckDBPyConnection  # Import base connection class

class HighlightingConnection(DuckDBPyConnection):
    """
    A DuckDB connection with optional SQL syntax highlighting.
    """
    def __init__(self, *args, **kwargs):
        self._highlight_enabled = kwargs.pop('highlight_enabled', False)
        super().__init__(*args, **kwargs)

    def sql(self, query, *, highlight=None):
        """
        Execute an SQL query with optional syntax highlighting.

        Args:
            query (str): The SQL query to execute.
            highlight (bool, optional): If True, print highlighted SQL. Defaults to None (uses global setting).

        Returns:
            DuckDBPyRelation: Result of the query.

        Raises:
            ImportError: If highlighting is requested but pygments is not installed.
        """
        should_highlight = highlight if highlight is not None else self._highlight_enabled
        if should_highlight:
            if not HAS_PYGMENTS:
                raise ImportError("SQL highlighting requires 'pygments'. Install with 'pip install pygments'.")
            highlighted = highlight(query, SqlLexer(), Terminal256Formatter())
            print(highlighted)
        return super().sql(query)

def connect_with_highlighting(*args, **kwargs):
    """
    Create a DuckDB connection with syntax highlighting support.

    Args:
        highlight_enabled (bool, optional): Enable highlighting by default. Defaults to False.
    """
    return HighlightingConnection(*args, **kwargs)