#include "main/client_context.hpp"

#include "main/database.hpp"

using namespace duckdb;
using namespace std;

ClientContext::ClientContext(DuckDB &database) : db(database), transaction(database.transaction_manager) {
}
