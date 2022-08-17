#include "duckdb/parser/tableref.hpp"

#include "duckdb/common/printer.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/parser/tableref/list.hpp"
#include "duckdb/common/to_string.hpp"

namespace duckdb {

string TableRef::BaseToString(string result) const {
	vector<string> column_name_alias;
	return BaseToString(move(result), column_name_alias);
}

string TableRef::BaseToString(string result, const vector<string> &column_name_alias) const {
	if (!alias.empty()) {
		result += " AS " + KeywordHelper::WriteOptionallyQuoted(alias);
	}
	if (!column_name_alias.empty()) {
		D_ASSERT(!alias.empty());
		result += "(";
		for (idx_t i = 0; i < column_name_alias.size(); i++) {
			if (i > 0) {
				result += ", ";
			}
			result += KeywordHelper::WriteOptionallyQuoted(column_name_alias[i]);
		}
		result += ")";
	}
	if (sample) {
		result += " TABLESAMPLE " + SampleMethodToString(sample->method);
		result += "(" + sample->sample_size.ToString() + " " + string(sample->is_percentage ? "PERCENT" : "ROWS") + ")";
		if (sample->seed >= 0) {
			result += "REPEATABLE (" + to_string(sample->seed) + ")";
		}
	}

	return result;
}

bool TableRef::Equals(const TableRef *other) const {
	return other && type == other->type && alias == other->alias &&
	       SampleOptions::Equals(sample.get(), other->sample.get());
}

void TableRef::Serialize(Serializer &serializer) const {
	FieldWriter writer(serializer);
	writer.WriteField<TableReferenceType>(type);
	writer.WriteString(alias);
	writer.WriteOptional(sample);
	Serialize(writer);
	writer.Finalize();
}

unique_ptr<TableRef> TableRef::Deserialize(Deserializer &source) {
	FieldReader reader(source);

	auto type = reader.ReadRequired<TableReferenceType>();
	auto alias = reader.ReadRequired<string>();
	auto sample = reader.ReadOptional<SampleOptions>(nullptr);
	unique_ptr<TableRef> result;
	switch (type) {
	case TableReferenceType::BASE_TABLE:
		result = BaseTableRef::Deserialize(reader);
		break;
	case TableReferenceType::CROSS_PRODUCT:
		result = CrossProductRef::Deserialize(reader);
		break;
	case TableReferenceType::JOIN:
		result = JoinRef::Deserialize(reader);
		break;
	case TableReferenceType::SUBQUERY:
		result = SubqueryRef::Deserialize(reader);
		break;
	case TableReferenceType::TABLE_FUNCTION:
		result = TableFunctionRef::Deserialize(reader);
		break;
	case TableReferenceType::EMPTY:
		result = EmptyTableRef::Deserialize(reader);
		break;
	case TableReferenceType::EXPRESSION_LIST:
		result = ExpressionListRef::Deserialize(reader);
		break;
	case TableReferenceType::CTE:
	case TableReferenceType::INVALID:
		throw InternalException("Unsupported type for TableRef::Deserialize");
	}
	reader.Finalize();

	result->alias = alias;
	result->sample = move(sample);
	return result;
}

void TableRef::CopyProperties(TableRef &target) const {
	D_ASSERT(type == target.type);
	target.alias = alias;
	target.query_location = query_location;
	target.sample = sample ? sample->Copy() : nullptr;
}

void TableRef::Print() {
	Printer::Print(ToString());
}

} // namespace duckdb
