#include "execution/operator/schema/physical_create_view.hpp"

#include "catalog/catalog_entry/schema_catalog_entry.hpp"
#include "execution/expression_executor.hpp"
#include "main/client_context.hpp"

using namespace duckdb;
using namespace std;

void PhysicalCreateView::_GetChunk(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	throw NotImplementedException("Eek");
}
