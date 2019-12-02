#include "duckdb/parser/tableref/table_function_ref.hpp"

#include "duckdb/common/serializer.hpp"

using namespace duckdb;
using namespace std;

bool TableFunctionRef::Equals(const TableRef *other_) const {
	if (!TableRef::Equals(other_)) {
		return false;
	}
	auto other = (TableFunctionRef *)other_;
	return function->Equals(other->function.get());
}

void TableFunctionRef::Serialize(Serializer &serializer) {
	TableRef::Serialize(serializer);
	function->Serialize(serializer);
}

unique_ptr<TableRef> TableFunctionRef::Deserialize(Deserializer &source) {
	auto result = make_unique<TableFunctionRef>();

	result->function = ParsedExpression::Deserialize(source);

	return move(result);
}

unique_ptr<TableRef> TableFunctionRef::Copy() {
	auto copy = make_unique<TableFunctionRef>();

	copy->function = function->Copy();
	copy->alias = alias;

	return move(copy);
}
