
#include "parser/tableref/table_function.hpp"

#include "common/serializer.hpp"

using namespace duckdb;
using namespace std;

void TableFunction::Serialize(Serializer &serializer) {
	TableRef::Serialize(serializer);
	function->Serialize(serializer);
}

unique_ptr<TableRef> TableFunction::Deserialize(Deserializer &source) {
	auto result = make_unique<TableFunction>();

	result->function = Expression::Deserialize(source);

	return result;
}

unique_ptr<TableRef> TableFunction::Copy() {
	auto copy = make_unique<TableFunction>();

	copy->function = function->Copy();
	copy->alias = alias;

	return copy;
}
