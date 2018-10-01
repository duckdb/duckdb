
#include "catalog/column_definition.hpp"

using namespace duckdb;
using namespace std;

ColumnDefinition::ColumnDefinition(string name, TypeId type)
    : name(name), type(type), has_default(false) {}

ColumnDefinition::ColumnDefinition(string name, TypeId type,
                                   Value default_value)
    : name(name), type(type), has_default(true), default_value(default_value) {}
