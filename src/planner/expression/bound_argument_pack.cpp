#include "duckdb/planner/expression/bound_argument_pack.hpp"

#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {

const vector<Vector> &ArgumentPack::GetInput(const Vector &pack) {
	D_ASSERT(IsPackType(pack.GetType()));
	return StructVector::GetEntries(pack);
}

const child_list_t<LogicalType>& ArgumentPack::GetTypes(const LogicalType &pack) {
	D_ASSERT(IsPackType(pack));
	return StructType::GetChildTypes(pack);
}

idx_t ArgumentPack::GetSize(const LogicalType &pack) {
	D_ASSERT(IsPackType(pack));
	return StructType::GetChildCount(pack);
}

bool ArgumentPack::IsPackType(const LogicalType &type) {
	return StructType::IsStruct(type) && type.GetAlias() == TYPE_ALIAS;
}

LogicalType ArgumentPack::PositionalType(vector<LogicalType> element_types) {
	return LogicalType::TUPLE(std::move(element_types)).WithAlias(TYPE_ALIAS);
}

LogicalType ArgumentPack::KeywordType(child_list_t<LogicalType> value_types) {
	return LogicalType::STRUCT(std::move(value_types)).WithAlias(TYPE_ALIAS);
}

unique_ptr<Expression> ArgumentPack::Create(vector<unique_ptr<Expression>> children, LogicalType pack_type) {
	D_ASSERT(IsPackType(pack_type));
	D_ASSERT(StructType::GetChildCount(pack_type) == children.size());
	auto result = make_uniq<BoundOperatorExpression>(ExpressionType::ARGUMENT_PACK, std::move(pack_type));
	result->GetChildrenMutable() = std::move(children);
	return std::move(result);
}

vector<unique_ptr<Expression>> &ArgumentPack::GetPackedChildren(Expression &pack) {
	D_ASSERT(IsPackType(pack.GetReturnType()));
	D_ASSERT(pack.GetExpressionClass() == ExpressionClass::BOUND_OPERATOR);
	return pack.Cast<BoundOperatorExpression>().GetChildrenMutable();
}

void ArgumentPack::RefreshType(Expression &pack) {
	auto &packed = GetPackedChildren(pack);
	auto &pack_type = pack.GetReturnType();

	if (pack_type.id() == LogicalTypeId::TUPLE) {
		vector<LogicalType> member_types;
		member_types.reserve(packed.size());
		for (auto &member : packed) {
			member_types.push_back(member->GetReturnType());
		}
		pack.SetReturnType(PositionalType(std::move(member_types)));
		return;
	}
	child_list_t<LogicalType> members;
	members.reserve(packed.size());
	for (idx_t i = 0; i < packed.size(); i++) {
		members.emplace_back(StructType::GetChildName(pack_type, i), packed[i]->GetReturnType());
	}
	pack.SetReturnType(KeywordType(std::move(members)));
}

bool ArgumentPack::Unroll(const vector<unique_ptr<Expression>> &children, const vector<LogicalType> &arguments,
                          vector<unique_ptr<Expression>> &flat_children, vector<LogicalType> &flat_arguments) {
	bool found_pack = false;
	for (auto &argument : arguments) {
		if (IsPackType(argument)) {
			found_pack = true;
			break;
		}
	}
	if (!found_pack) {
		return false;
	}

	// Reading the unrolled list back binds every trailing argument into the "*args" pack, so the only calls that
	// survive the round-trip are the ones that look like a pre-pack variadic call: leading positional arguments
	// followed by the pack. A "**kwargs" pack or a keyword-only argument sitting behind the "*args" one cannot be
	// put back where it belongs, so it is not representable in a storage version that predates argument packs.
	for (idx_t i = 0; i < arguments.size(); i++) {
		if (!IsPackType(arguments[i])) {
			continue;
		}
		if (arguments[i].id() == LogicalTypeId::STRUCT) {
			throw SerializationException("Cannot serialize a call to a function with a '**kwargs' parameter to a "
			                             "storage version that predates argument packs");
		}
		if (i + 1 != arguments.size()) {
			throw SerializationException("Cannot serialize a call to a function with arguments after its '*args' "
			                             "parameter to a storage version that predates argument packs");
		}
	}

	for (idx_t i = 0; i < children.size(); i++) {
		const auto &pack_type = i < arguments.size() ? arguments[i] : children[i]->GetReturnType();
		if (!IsPackType(pack_type)) {
			flat_children.push_back(children[i]->Copy());
			flat_arguments.push_back(pack_type);
			continue;
		}
		const auto child_count = StructType::GetChildCount(pack_type);

		vector<unique_ptr<Expression>> packed;
		if (children[i]->GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
			// the pack collected only constants and was folded into a single struct value
			auto &value = children[i]->Cast<BoundConstantExpression>().GetValue();
			for (auto &packed_value : StructValue::GetChildren(value)) {
				packed.push_back(make_uniq<BoundConstantExpression>(packed_value));
			}
		} else if (children[i]->GetExpressionClass() == ExpressionClass::BOUND_OPERATOR) {
			for (auto &packed_child : children[i]->Cast<BoundOperatorExpression>().GetChildren()) {
				packed.push_back(packed_child->Copy());
			}
		} else {
			throw SerializationException(
			    "Cannot serialize an argument pack that was replaced by a %s expression to this storage version",
			    ExpressionTypeToString(children[i]->GetExpressionType()));
		}
		D_ASSERT(packed.size() == child_count);

		for (idx_t child_idx = 0; child_idx < child_count; child_idx++) {
			flat_arguments.push_back(StructType::GetChildType(pack_type, child_idx));
			flat_children.push_back(std::move(packed[child_idx]));
		}
	}
	return true;
}

} // namespace duckdb
