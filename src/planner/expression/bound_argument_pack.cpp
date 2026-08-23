#include "duckdb/planner/expression/bound_argument_pack.hpp"

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {

const vector<Vector> &ArgumentPack::GetInput(const Vector &pack) {
	D_ASSERT(IsPackType(pack.GetType()));
	return StructVector::GetEntries(pack);
}

vector<Vector> &ArgumentPack::GetInput(Vector &pack) {
	D_ASSERT(IsPackType(pack.GetType()));
	return StructVector::GetEntries(pack);
}

const child_list_t<LogicalType> &ArgumentPack::GetTypes(const LogicalType &pack) {
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

bool ArgumentPack::IsPack(const Expression &expr) {
	return expr.GetExpressionType() == ExpressionType::ARGUMENT_PACK;
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

// Whether the pack in argument slot 'index' can be flattened into the argument list and rolled back up from it,
// and if not, why not. Returns nullptr when it can.
static const char *UnrollBlocker(const vector<unique_ptr<Expression>> &children, const vector<LogicalType> &arguments,
                                 idx_t index) {
	if (index + 1 != arguments.size()) {
		// an argument sitting behind the pack cannot be put back where it belongs
		return "the function takes arguments after its '*args'/'**kwargs' parameter";
	}
	if (index >= children.size()) {
		return "the argument pack has no expression";
	}
	auto &child = *children[index];
	// the pack collected only constants and was folded into a single struct value, or it is still a pack
	if (child.GetExpressionType() == ExpressionType::VALUE_CONSTANT ||
	    child.GetExpressionClass() == ExpressionClass::BOUND_OPERATOR) {
		return nullptr;
	}
	return "the argument pack was replaced by another expression";
}

bool ArgumentPack::Unroll(Serializer &serializer, const vector<unique_ptr<Expression>> &children,
                          const vector<LogicalType> &arguments, vector<unique_ptr<Expression>> &flat_children,
                          vector<LogicalType> &flat_arguments) {
	bool found_pack = false;
	for (idx_t i = 0; i < arguments.size(); i++) {
		if (!IsPackType(arguments[i])) {
			continue;
		}
		found_pack = true;
		auto blocker = UnrollBlocker(children, arguments, i);
		if (!blocker) {
			continue;
		}
		if (serializer.ShouldSerialize(StorageVersion::V2_0_0)) {
			// the call keeps its packs - this storage version can represent them
			return false;
		}
		throw SerializationException("Cannot serialize this call to a storage version that predates argument packs: "
		                             "%s",
		                             blocker);
	}
	if (!found_pack) {
		return false;
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
			auto &value = children[i]->Cast<BoundConstantExpression>().GetValue();
			for (auto &packed_value : StructValue::GetChildren(value)) {
				packed.push_back(make_uniq<BoundConstantExpression>(packed_value));
			}
		} else {
			for (auto &packed_child : children[i]->Cast<BoundOperatorExpression>().GetChildren()) {
				packed.push_back(packed_child->Copy());
			}
		}
		D_ASSERT(packed.size() == child_count);

		// the flat encoding of a keyword argument keeps its name in the expression alias
		const auto is_keyword_pack = pack_type.id() == LogicalTypeId::STRUCT;
		for (idx_t child_idx = 0; child_idx < child_count; child_idx++) {
			if (is_keyword_pack) {
				packed[child_idx]->SetAlias(StructType::GetChildName(pack_type, child_idx));
			}
			flat_arguments.push_back(StructType::GetChildType(pack_type, child_idx));
			flat_children.push_back(std::move(packed[child_idx]));
		}
	}
	return true;
}

static vector<LogicalType> ExtractTypes(const child_list_t<LogicalType> &fields) {
	vector<LogicalType> types;
	types.reserve(fields.size());
	for (auto &field : fields) {
		types.push_back(field.second);
	}
	return types;
}

bool ArgumentPack::Reroll(const FunctionSignature &sig, vector<unique_ptr<Expression>> &children,
                          vector<LogicalType> &arguments) {
	auto keyword_index = sig.GetVarKeywordIndex();
	auto positional_index = sig.GetVarPositionalIndex();
	if (keyword_index.IsValid() == positional_index.IsValid()) {
		// only the shape Unroll produces can be rolled back up: a single trailing pack
		return false;
	}
	const auto is_keyword_pack = keyword_index.IsValid();
	const auto leading = is_keyword_pack ? keyword_index.GetIndex() : positional_index.GetIndex();
	if (leading + 1 != sig.GetParameterCount() || children.size() < leading) {
		return false;
	}
	if (children.size() == sig.GetParameterCount() && IsPackType(children[leading]->GetReturnType())) {
		// this call is already packed
		return false;
	}

	vector<unique_ptr<Expression>> packed;
	child_list_t<LogicalType> fields;
	for (idx_t i = leading; i < children.size(); i++) {
		fields.emplace_back(is_keyword_pack ? children[i]->GetAlias() : Identifier(), children[i]->GetReturnType());
		packed.push_back(std::move(children[i]));
	}
	auto pack_type = is_keyword_pack ? KeywordType(std::move(fields)) : PositionalType(ExtractTypes(fields));

	children.resize(leading);
	arguments.resize(MinValue<idx_t>(arguments.size(), leading));
	children.push_back(Create(std::move(packed), pack_type));
	arguments.push_back(std::move(pack_type));
	return true;
}

} // namespace duckdb
