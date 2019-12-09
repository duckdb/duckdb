#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/execution/physical_operator.hpp"

using namespace duckdb;
using namespace std;

PreparedStatementData::PreparedStatementData(StatementType type) : statement_type(type) {

}

PreparedStatementData::~PreparedStatementData() {

}