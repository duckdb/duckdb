#include "duckdb/parser/query_node/set_operation_node.hpp"

#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/serializer/enum_serializer.hpp"

namespace duckdb {

string SetOperationNode::ToString() const {
	string result;
	result = cte_map.ToString();
	result += "(" + left->ToString() + ") ";
	bool is_distinct = false;
	for (idx_t modifier_idx = 0; modifier_idx < modifiers.size(); modifier_idx++) {
		if (modifiers[modifier_idx]->type == ResultModifierType::DISTINCT_MODIFIER) {
			is_distinct = true;
			break;
		}
	}

	switch (setop_type) {
	case SetOperationType::UNION:
		result += is_distinct ? "UNION" : "UNION ALL";
		break;
	case SetOperationType::UNION_BY_NAME:
		result += is_distinct ? "UNION BY NAME" : "UNION ALL BY NAME";
		break;
	case SetOperationType::EXCEPT:
		D_ASSERT(is_distinct);
		result += "EXCEPT";
		break;
	case SetOperationType::INTERSECT:
		D_ASSERT(is_distinct);
		result += "INTERSECT";
		break;
	default:
		throw InternalException("Unsupported set operation type");
	}
	result += " (" + right->ToString() + ")";
	return result + ResultModifiersToString();
}

bool SetOperationNode::Equals(const QueryNode *other_p) const {
	if (!QueryNode::Equals(other_p)) {
		return false;
	}
	if (this == other_p) {
		return true;
	}
	auto other = (SetOperationNode *)other_p;
	if (setop_type != other->setop_type) {
		return false;
	}
	if (!left->Equals(other->left.get())) {
		return false;
	}
	if (!right->Equals(other->right.get())) {
		return false;
	}
	return true;
}

unique_ptr<QueryNode> SetOperationNode::Copy() const {
	auto result = make_unique<SetOperationNode>();
	result->setop_type = setop_type;
	result->left = left->Copy();
	result->right = right->Copy();
	this->CopyProperties(*result);
	return std::move(result);
}

void SetOperationNode::Serialize(FieldWriter &writer) const {
	writer.WriteField<SetOperationType>(setop_type);
	writer.WriteSerializable(*left);
	writer.WriteSerializable(*right);
}

unique_ptr<QueryNode> SetOperationNode::Deserialize(FieldReader &reader) {
	auto result = make_unique<SetOperationNode>();
	result->setop_type = reader.ReadRequired<SetOperationType>();
	result->left = reader.ReadRequiredSerializable<QueryNode>();
	result->right = reader.ReadRequiredSerializable<QueryNode>();
	return std::move(result);
}

template<> const char* EnumSerializer::EnumToString(SetOperationType value) {
	switch (value) {
	case SetOperationType::NONE:
		return "NONE";
	case SetOperationType::UNION:
		return "UNION";
	case SetOperationType::EXCEPT:
		return "EXCEPT";
	case SetOperationType::INTERSECT:
		return "INTERSECT";
	case SetOperationType::UNION_BY_NAME:
		return "UNION_BY_NAME";
	default:
		throw NotImplementedException("EnumToString not implemented for enum value");
	}
}

template<> SetOperationType EnumSerializer::StringToEnum(const char *value) {
	if (strcmp(value, "NONE") == 0) {
		return SetOperationType::NONE;
	} else if (strcmp(value, "UNION") == 0) {
		return SetOperationType::UNION;
	} else if (strcmp(value, "EXCEPT") == 0) {
		return SetOperationType::EXCEPT;
	} else if (strcmp(value, "INTERSECT") == 0) {
		return SetOperationType::INTERSECT;
	} else if (strcmp(value, "UNION_BY_NAME") == 0) {
		return SetOperationType::UNION_BY_NAME;
	} else {
		throw NotImplementedException("StringToEnum not implemented for enum value");
	}
}

void SetOperationNode::FormatSerialize(duckdb::FormatSerializer &serializer) const {
	QueryNode::FormatSerialize(serializer);
	serializer.WriteProperty("set_op_type", setop_type);
	serializer.WriteProperty("left", *left);
	serializer.WriteProperty("right", *right);
}

unique_ptr<QueryNode> SetOperationNode::FormatDeserialize(duckdb::FormatDeserializer &deserializer) {
	auto result = make_unique<SetOperationNode>();
	deserializer.ReadProperty("set_op_type", result->setop_type);
	deserializer.ReadProperty("left", result->left);
	deserializer.ReadProperty("right", result->right);
	return std::move(result);
}

} // namespace duckdb
