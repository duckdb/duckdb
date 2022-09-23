#include "duckdb/parser/expression/function_expression.hpp"

#include <utility>
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/types/hash.hpp"

namespace duckdb {

FunctionExpression::FunctionExpression(string schema, const string &function_name,
                                       vector<unique_ptr<ParsedExpression>> children_p,
                                       unique_ptr<ParsedExpression> filter, unique_ptr<OrderModifier> order_bys_p,
                                       bool distinct, bool is_operator, bool export_state_p)
    : ParsedExpression(ExpressionType::FUNCTION, ExpressionClass::FUNCTION), schema(std::move(schema)),
      function_name(StringUtil::Lower(function_name)), is_operator(is_operator), children(move(children_p)),
      distinct(distinct), filter(move(filter)), order_bys(move(order_bys_p)), export_state(export_state_p) {
	D_ASSERT(!function_name.empty());
	if (!order_bys) {
		order_bys = make_unique<OrderModifier>();
	}
}

FunctionExpression::FunctionExpression(const string &function_name, vector<unique_ptr<ParsedExpression>> children_p,
                                       unique_ptr<ParsedExpression> filter, unique_ptr<OrderModifier> order_bys,
                                       bool distinct, bool is_operator, bool export_state_p)
    : FunctionExpression(INVALID_SCHEMA, function_name, move(children_p), move(filter), move(order_bys), distinct,
                         is_operator, export_state_p) {
}

string FunctionExpression::ToString() const {
	return ToString<FunctionExpression, ParsedExpression>(*this, schema, function_name, is_operator, distinct,
	                                                      filter.get(), order_bys.get(), export_state, true);
}

bool FunctionExpression::Equals(const FunctionExpression *a, const FunctionExpression *b) {
	if (a->schema != b->schema || a->function_name != b->function_name || b->distinct != a->distinct) {
		return false;
	}
	if (b->children.size() != a->children.size()) {
		return false;
	}
	for (idx_t i = 0; i < a->children.size(); i++) {
		if (!a->children[i]->Equals(b->children[i].get())) {
			return false;
		}
	}
	if (!BaseExpression::Equals(a->filter.get(), b->filter.get())) {
		return false;
	}
	if (!a->order_bys->Equals(b->order_bys.get())) {
		return false;
	}
	if (a->export_state != b->export_state) {
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
	for (auto &child : children) {
		copy_children.push_back(child->Copy());
	}
	if (filter) {
		filter_copy = filter->Copy();
	}
	unique_ptr<OrderModifier> order_copy;
	if (order_bys) {
		order_copy.reset(static_cast<OrderModifier *>(order_bys->Copy().release()));
	}

	auto copy = make_unique<FunctionExpression>(function_name, move(copy_children), move(filter_copy), move(order_copy),
	                                            distinct, is_operator, export_state);
	copy->schema = schema;
	copy->CopyProperties(*this);
	return move(copy);
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

	unique_ptr<FunctionExpression> function;
	function = make_unique<FunctionExpression>(function_name, move(children), move(filter), move(order_bys), distinct,
	                                           is_operator, export_state);
	function->schema = schema;
	return move(function);
}

void FunctionExpression::Verify() const {
	D_ASSERT(!function_name.empty());
}

} // namespace duckdb
