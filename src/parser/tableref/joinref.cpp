#include "duckdb/parser/tableref/joinref.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

string JoinRef::ToString() const {
	string result;
	result = left->ToString() + " ";
	switch (ref_type) {
	case JoinRefType::REGULAR:
		result += JoinTypeToString(type) + " JOIN ";
		break;
	case JoinRefType::NATURAL:
		result += "NATURAL ";
		result += JoinTypeToString(type) + " JOIN ";
		break;
	case JoinRefType::CROSS:
		result += ", ";
		break;
	case JoinRefType::POSITIONAL:
		result += "POSITIONAL JOIN ";
		break;
	}
	result += right->ToString();
	if (condition) {
		D_ASSERT(using_columns.empty());
		result += " ON (";
		result += condition->ToString();
		result += ")";
	} else if (!using_columns.empty()) {
		result += " USING (";
		for (idx_t i = 0; i < using_columns.size(); i++) {
			if (i > 0) {
				result += ", ";
			}
			result += using_columns[i];
		}
		result += ")";
	}
	return result;
}

bool JoinRef::Equals(const TableRef *other_p) const {
	if (!TableRef::Equals(other_p)) {
		return false;
	}
	auto other = (JoinRef *)other_p;
	if (using_columns.size() != other->using_columns.size()) {
		return false;
	}
	for (idx_t i = 0; i < using_columns.size(); i++) {
		if (using_columns[i] != other->using_columns[i]) {
			return false;
		}
	}
	return left->Equals(other->left.get()) && right->Equals(other->right.get()) &&
	       BaseExpression::Equals(condition.get(), other->condition.get()) && type == other->type;
}

unique_ptr<TableRef> JoinRef::Copy() {
	auto copy = make_unique<JoinRef>(ref_type);
	copy->left = left->Copy();
	copy->right = right->Copy();
	if (condition) {
		copy->condition = condition->Copy();
	}
	copy->type = type;
	copy->ref_type = ref_type;
	copy->alias = alias;
	copy->using_columns = using_columns;
	return std::move(copy);
}

void JoinRef::Serialize(FieldWriter &writer) const {
	writer.WriteSerializable(*left);
	writer.WriteSerializable(*right);
	writer.WriteOptional(condition);
	writer.WriteField<JoinType>(type);
	writer.WriteField<JoinRefType>(ref_type);
	writer.WriteList<string>(using_columns);
}

unique_ptr<TableRef> JoinRef::Deserialize(FieldReader &reader) {
	auto result = make_unique<JoinRef>(JoinRefType::REGULAR);
	result->left = reader.ReadRequiredSerializable<TableRef>();
	result->right = reader.ReadRequiredSerializable<TableRef>();
	result->condition = reader.ReadOptional<ParsedExpression>(nullptr);
	result->type = reader.ReadRequired<JoinType>();
	result->ref_type = reader.ReadRequired<JoinRefType>();
	result->using_columns = reader.ReadRequiredList<string>();
	return std::move(result);
}

} // namespace duckdb
