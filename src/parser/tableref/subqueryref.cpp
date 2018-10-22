
#include "parser/tableref/subqueryref.hpp"

#include "common/serializer.hpp"

using namespace duckdb;
using namespace std;

SubqueryRef::SubqueryRef(unique_ptr<SelectStatement> subquery_)
    : TableRef(TableReferenceType::SUBQUERY), subquery(move(subquery_)) {
}

unique_ptr<TableRef> SubqueryRef::Copy() {
	assert(!context);

	auto copy = make_unique<SubqueryRef>(subquery->Copy());
	copy->alias = alias;
	return copy;
}

void SubqueryRef::Serialize(Serializer &serializer) {
	assert(!context);

	TableRef::Serialize(serializer);
	subquery->Serialize(serializer);
}

unique_ptr<TableRef> SubqueryRef::Deserialize(Deserializer &source) {
	auto subquery = SelectStatement::Deserialize(source);
	if (!subquery) {
		return nullptr;
	}

	return make_unique<SubqueryRef>(move(subquery));
}
