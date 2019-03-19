#include "parser/tableref/table_function.hpp"

#include "common/serializer.hpp"

using namespace duckdb;
using namespace std;

bool TableFunction::Equals(const TableRef *other_) const {
	if (!TableRef::Equals(other_)) {
		return false;
	}
	auto other = (TableFunction *)other_;
	return function->Equals(other->function.get());
}

void TableFunction::Serialize(Serializer &serializer) {
	TableRef::Serialize(serializer);
	function->Serialize(serializer);
}

unique_ptr<TableRef> TableFunction::Deserialize(Deserializer &source) {
	auto result = make_unique<TableFunction>();

	result->function = ParsedExpression::Deserialize(source);

	return move(result);
}

unique_ptr<TableRef> TableFunction::Copy() {
	auto copy = make_unique<TableFunction>();

	copy->function = function->Copy();
	copy->alias = alias;

	return move(copy);
}
