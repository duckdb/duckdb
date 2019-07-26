#include "parser/expression/aggregate_expression.hpp"

#include "common/serializer.hpp"
#include "common/string_util.hpp"
#include "common/types/hash.hpp"

using namespace duckdb;
using namespace std;

AggregateExpression::AggregateExpression(string schema, string aggregate_name, bool distinct, unique_ptr<ParsedExpression> child)
    : ParsedExpression(ExpressionType::AGGREGATE, ExpressionClass::AGGREGATE)
    , schema(schema)
    , aggregate_name(StringUtil::Lower(aggregate_name))
    , distinct(distinct)
{
	this->child = move(child);
}

AggregateExpression::AggregateExpression(string aggregate_name, bool distinct, unique_ptr<ParsedExpression> child)
    : AggregateExpression(DEFAULT_SCHEMA, aggregate_name, distinct, move(child)) {
}

string AggregateExpression::ToString() const {
	return aggregate_name + "(" + (distinct ? "DISTINCT " : " " ) + (child ? child->ToString() : string()) + ")";
}

bool AggregateExpression::Equals(const BaseExpression *other_) const {
	if (!BaseExpression::Equals(other_)) {
		return false;
	}
	auto other = (AggregateExpression *)other_;
	if (schema != other->schema) {
		return false;
	}
	if (aggregate_name != other->aggregate_name) {
		return false;
	}
	if (distinct != other->distinct) {
		return false;
	}
	if (child) {
		if (!child->Equals(other->child.get())) {
			return false;
		}
	} else if (other->child) {
		return false;
	}
	return true;
}

uint64_t AggregateExpression::Hash() const {
	uint64_t result = ParsedExpression::Hash();
	result = CombineHash(result, duckdb::Hash<const char *>(schema.c_str()));
	result = CombineHash(result, duckdb::Hash<const char *>(aggregate_name.c_str()));
	result = CombineHash(result, duckdb::Hash<bool>(distinct));
	return result;
}

unique_ptr<ParsedExpression> AggregateExpression::Copy() const {
	auto new_child = child ? child->Copy() : nullptr;
	auto new_aggregate = make_unique<AggregateExpression>(schema, aggregate_name, distinct, move(new_child));
	new_aggregate->CopyProperties(*this);
	return move(new_aggregate);
}

void AggregateExpression::Serialize(Serializer &serializer) {
	ParsedExpression::Serialize(serializer);
	serializer.WriteString(aggregate_name);
	serializer.WriteString(schema);
	serializer.Write<bool>(distinct);
	serializer.WriteOptional(child);
}

unique_ptr<ParsedExpression> AggregateExpression::Deserialize(ExpressionType type, Deserializer &source) {
	auto aggregate_name = source.Read<string>();
	auto schema = source.Read<string>();
	auto distinct = source.Read<bool>();
	auto child = source.ReadOptional<ParsedExpression>();
	return make_unique<AggregateExpression>(schema, aggregate_name, distinct, move(child));
}
