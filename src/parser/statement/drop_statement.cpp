#include "parser/statement/drop_statement.hpp"

#include "common/assert.hpp"

using namespace duckdb;
using namespace std;

string DropStatement::ToString() const { return "Drop"; }
