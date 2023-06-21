#include "duckdb/parser/expression/function_expression.hpp"

#include <utility>
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/types/hash.hpp"

#include "duckdb/common/serializer/format_serializer.hpp"
#include "duckdb/common/serializer/format_deserializer.hpp"

namespace duckdb {

FunctionExpression::FunctionExpression(string catalog, string schema, const string &function_name,
                                       vector<unique_ptr<ParsedExpression>> children_p,
                                       unique_ptr<ParsedExpression> filter, unique_ptr<OrderModifier> order_bys_p,
                                       bool distinct, bool is_operator, bool export_state_p)
    : ParsedExpression(ExpressionType::FUNCTION, ExpressionClass::FUNCTION), catalog(std::move(catalog)),
      schema(std::move(schema)), function_name(StringUtil::Lower(function_name)), is_operator(is_operator),
      children(std::move(children_p)), distinct(distinct), filter(std::move(filter)), order_bys(std::move(order_bys_p)),
      export_state(export_state_p) {
	D_ASSERT(!function_name.empty());
	if (!order_bys) {
		order_bys = make_uniq<OrderModifier>();
	}
}

FunctionExpression::FunctionExpression(const string &function_name, vector<unique_ptr<ParsedExpression>> children_p,
                                       unique_ptr<ParsedExpression> filter, unique_ptr<OrderModifier> order_bys,
                                       bool distinct, bool is_operator, bool export_state_p)
    : FunctionExpression(INVALID_CATALOG, INVALID_SCHEMA, function_name, std::move(children_p), std::move(filter),
                         std::move(order_bys), distinct, is_operator, export_state_p) {
}

string FunctionExpression::ToString() const {
	return ToString<FunctionExpression, ParsedExpression>(*this, schema, function_name, is_operator, distinct,
	                                                      filter.get(), order_bys.get(), export_state, true);
}

bool FunctionExpression::Equal(const FunctionExpression &a, const FunctionExpression &b) {
	if (a.catalog != b.catalog || a.schema != b.schema || a.function_name != b.function_name ||
	    b.distinct != a.distinct) {
		return false;
	}
	if (b.children.size() != a.children.size()) {
		return false;
	}
	for (idx_t i = 0; i < a.children.size(); i++) {
		if (!a.children[i]->Equals(*b.children[i])) {
			return false;
		}
	}
	if (!ParsedExpression::Equals(a.filter, b.filter)) {
		return false;
	}
	if (!OrderModifier::Equals(a.order_bys, b.order_bys)) {
		return false;
	}
	if (a.export_state != b.export_state) {
		return false;
	}
	return true;
}

hash_t FunctionExpression::Hash() const {
	hash_t result = ParsedExpression::Hash();
	result = CombineHash(result, duckdb::Hash<const char *>(schema.c_str()));
	result = CombineHash(result, duckdb::Hash<const char *>(function_name.c_str()));
	result = CombineHash(result, duckdb::Hash<bool>(distinct));
	result = CombineHash(result, duckdb::Hash<bool>(export_state));
	return result;
}

unique_ptr<ParsedExpression> FunctionExpression::Copy() const {
	vector<unique_ptr<ParsedExpression>> copy_children;
	unique_ptr<ParsedExpression> filter_copy;
	copy_children.reserve(children.size());
	for (auto &child : children) {
		copy_children.push_back(child->Copy());
	}
	if (filter) {
		filter_copy = filter->Copy();
	}
	auto order_copy = order_bys ? unique_ptr_cast<ResultModifier, OrderModifier>(order_bys->Copy()) : nullptr;
	auto copy =
	    make_uniq<FunctionExpression>(catalog, schema, function_name, std::move(copy_children), std::move(filter_copy),
	                                  std::move(order_copy), distinct, is_operator, export_state);
	copy->CopyProperties(*this);
	return std::move(copy);
}

void FunctionExpression::Serialize(FieldWriter &writer) const {
	writer.WriteString(function_name);
	writer.WriteString(schema);
	writer.WriteSerializableList(children);
	writer.WriteOptional(filter);
	writer.WriteSerializable((ResultModifier &)*order_bys);
	writer.WriteField<bool>(distinct);
	writer.WriteField<bool>(is_operator);
	writer.WriteField<bool>(export_state);
	writer.WriteString(catalog);
}

unique_ptr<ParsedExpression> FunctionExpression::Deserialize(ExpressionType type, FieldReader &reader) {
	auto function_name = reader.ReadRequired<string>();
	auto schema = reader.ReadRequired<string>();
	auto children = reader.ReadRequiredSerializableList<ParsedExpression>();
	auto filter = reader.ReadOptional<ParsedExpression>(nullptr);
	auto order_bys = unique_ptr_cast<ResultModifier, OrderModifier>(reader.ReadRequiredSerializable<ResultModifier>());
	auto distinct = reader.ReadRequired<bool>();
	auto is_operator = reader.ReadRequired<bool>();
	auto export_state = reader.ReadField<bool>(false);
	auto catalog = reader.ReadField<string>(INVALID_CATALOG);

	unique_ptr<FunctionExpression> function;
	function = make_uniq<FunctionExpression>(catalog, schema, function_name, std::move(children), std::move(filter),
	                                         std::move(order_bys), distinct, is_operator, export_state);
	return std::move(function);
}

void FunctionExpression::Verify() const {
	D_ASSERT(!function_name.empty());
}

void FunctionExpression::FormatSerialize(FormatSerializer &serializer) const {
	ParsedExpression::FormatSerialize(serializer);
	serializer.WriteProperty("function_name", function_name);
	serializer.WriteProperty("schema", schema);
	serializer.WriteProperty("children", children);
	serializer.WriteOptionalProperty("filter", filter);
	serializer.WriteProperty("order_bys", (ResultModifier &)*order_bys);
	serializer.WriteProperty("distinct", distinct);
	serializer.WriteProperty("is_operator", is_operator);
	serializer.WriteProperty("export_state", export_state);
	serializer.WriteProperty("catalog", catalog);
}

unique_ptr<ParsedExpression> FunctionExpression::FormatDeserialize(ExpressionType type,
                                                                   FormatDeserializer &deserializer) {
	auto function_name = deserializer.ReadProperty<string>("function_name");
	auto schema = deserializer.ReadProperty<string>("schema");
	auto children = deserializer.ReadProperty<vector<unique_ptr<ParsedExpression>>>("children");
	auto filter = deserializer.ReadOptionalProperty<unique_ptr<ParsedExpression>>("filter");
	auto order_bys = unique_ptr_cast<ResultModifier, OrderModifier>(
	    deserializer.ReadProperty<unique_ptr<ResultModifier>>("order_bys"));
	auto distinct = deserializer.ReadProperty<bool>("distinct");
	auto is_operator = deserializer.ReadProperty<bool>("is_operator");
	auto export_state = deserializer.ReadProperty<bool>("export_state");
	auto catalog = deserializer.ReadProperty<string>("catalog");

	unique_ptr<FunctionExpression> function;
	function = make_uniq<FunctionExpression>(catalog, schema, function_name, std::move(children), std::move(filter),
	                                         std::move(order_bys), distinct, is_operator, export_state);
	return std::move(function);
}

} // namespace duckdb
