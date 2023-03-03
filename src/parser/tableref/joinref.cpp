#include "duckdb/parser/tableref/joinref.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/serializer/enum_serializer.hpp"

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

template<> const char* EnumSerializer::EnumToString(JoinRefType value) {
	switch (value) {
	case JoinRefType::REGULAR:
		return "REGULAR";
	case JoinRefType::NATURAL:
		return "NATURAL";
	case JoinRefType::CROSS:
		return "CROSS";
	case JoinRefType::POSITIONAL:
		return "POSITIONAL";
	default:
		throw NotImplementedException("ToString not implemented for enum value");
	}}

template<> JoinRefType EnumSerializer::StringToEnum(const char *value) {
	if(strcmp(value, "REGULAR") == 0) {
		return JoinRefType::REGULAR;
	} else if(strcmp(value, "NATURAL") == 0) {
		return JoinRefType::NATURAL;
	} else if(strcmp(value, "CROSS") == 0) {
		return JoinRefType::CROSS;
	} else if(strcmp(value, "POSITIONAL") == 0) {
		return JoinRefType::POSITIONAL;
	} else {
		throw NotImplementedException("StringToEnum not implemented for enum value");
	}
}

void JoinRef::FormatSerialize(FormatSerializer &serializer) const {
	TableRef::FormatSerialize(serializer);
	serializer.WriteProperty("left", *left);
	serializer.WriteProperty("right", *right);
	serializer.WriteOptionalProperty("condition", condition);
	serializer.WriteProperty("join_type", type);
	serializer.WriteProperty("ref_type", ref_type);
	serializer.WriteProperty("using_columns", using_columns);
}

unique_ptr<TableRef> JoinRef::FormatDeserialize(FormatDeserializer &source) {
	auto result = make_unique<JoinRef>(JoinRefType::REGULAR);

	source.ReadProperty("left", result->left);
	source.ReadProperty("right", result->right);
	source.ReadOptionalProperty("condition", result->condition);
	source.ReadProperty("join_type", result->type);
	source.ReadProperty("ref_type", result->ref_type);
	source.ReadProperty("using_columns", result->using_columns);

	return std::move(result);
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
