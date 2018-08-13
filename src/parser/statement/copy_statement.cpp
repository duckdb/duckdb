#include "parser/statement/copy_statement.hpp"

#include "common/assert.hpp"

using namespace duckdb;
using namespace std;

string CopyStatement::ToString() const { return "Copy"; }
