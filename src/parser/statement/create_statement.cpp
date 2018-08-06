#include "parser/statement/create_statement.hpp"

#include "common/assert.hpp"

using namespace duckdb;
using namespace std;

string CreateStatement::ToString() const { return "Create"; }
