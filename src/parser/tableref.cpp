
#include "parser/tableref.hpp"
#include "parser/tableref/list.hpp"

#include "common/serializer.hpp"

using namespace duckdb;
using namespace std;

void TableRef::Serialize(Serializer &serializer) {
	serializer.Write<TableReferenceType>(type);
	serializer.WriteString(alias);
}

//! Deserializes a blob back into an TableRef
unique_ptr<TableRef> TableRef::Deserialize(Deserializer &source) {
	bool failed = false;
	auto type = source.Read<TableReferenceType>(failed);
	auto alias = source.Read<string>(failed);
	if (failed) {
		return nullptr;
	}
	unique_ptr<TableRef> result;
	switch (type) {
	case TableReferenceType::BASE_TABLE:
		result = BaseTableRef::Deserialize(source);
		break;
	case TableReferenceType::CROSS_PRODUCT:
		result = CrossProductRef::Deserialize(source);
		break;
	case TableReferenceType::JOIN:
		result = JoinRef::Deserialize(source);
		break;
	case TableReferenceType::SUBQUERY:
		result = SubqueryRef::Deserialize(source);
		break;
	case TableReferenceType::INVALID:
		return nullptr;
	}
	if (!result) {
		return nullptr;
	}
	result->alias = alias;
	return result;
}