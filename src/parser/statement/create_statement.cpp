
#include "parser/statement/insert_statement.hpp"

#include "common/assert.hpp"

using namespace duckdb;
using namespace std;

string InsertStatement::ToString() const { return "Insert"; }
