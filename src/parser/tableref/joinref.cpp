#include "duckdb/parser/tableref/joinref.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

string JoinRef::ToString() const {
	string result;
	result = left->ToString() + " ";
	switch (ref_type) {
	case JoinRefType::REGULAR:
		result += EnumUtil::ToString(type) + " JOIN ";
		break;
	case JoinRefType::NATURAL:
		result += "NATURAL ";
		result += EnumUtil::ToString(type) + " JOIN ";
		break;
	case JoinRefType::ASOF:
		result += "ASOF ";
		result += EnumUtil::ToString(type) + " JOIN ";
		break;
	case JoinRefType::CROSS:
		result += ", ";
		break;
	case JoinRefType::POSITIONAL:
		result += "POSITIONAL JOIN ";
		break;
	case JoinRefType::DEPENDENT:
		result += "DEPENDENT JOIN ";
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

bool JoinRef::Equals(const TableRef &other_p) const {
	if (!TableRef::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<JoinRef>();
	if (using_columns.size() != other.using_columns.size()) {
		return false;
	}
	for (idx_t i = 0; i < using_columns.size(); i++) {
		if (using_columns[i] != other.using_columns[i]) {
			return false;
		}
	}
	return left->Equals(*other.left) && right->Equals(*other.right) &&
	       ParsedExpression::Equals(condition, other.condition) && type == other.type;
}

unique_ptr<TableRef> JoinRef::Copy() {
	auto copy = make_uniq<JoinRef>(ref_type);
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

} // namespace duckdb
