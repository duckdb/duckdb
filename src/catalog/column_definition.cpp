
#include "catalog/column_definition.hpp"

using namespace duckdb;
using namespace std;

ColumnDefinition::ColumnDefinition(string name, TypeId type, bool is_not_null)
    : name(name), type(type), is_not_null(is_not_null), has_default(false) {}

ColumnDefinition::ColumnDefinition(string name, TypeId type, bool is_not_null,
                                   Value default_value)
    : name(name), type(type), is_not_null(is_not_null), has_default(true),
      default_value(default_value) {}
