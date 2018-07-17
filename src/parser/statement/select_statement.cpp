
#include "parser/statement/select_statement.hpp"

#include "common/assert.hpp"

using namespace duckdb;
using namespace std;

string SelectStatement::ToString() const { return "Select"; }
