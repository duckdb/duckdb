
#include "parser/tableref/subqueryref.hpp"

using namespace duckdb;
using namespace std;

SubqueryRef::SubqueryRef(unique_ptr<SelectStatement> subquery_)
    : TableRef(TableReferenceType::SUBQUERY), subquery(move(subquery_)) {}
