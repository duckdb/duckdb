#include "duckdb/planner/compiler_result.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {

bool CompilerPathComponent::operator==(const CompilerPathComponent &other) const {
	return type == other.type && ordinal == other.ordinal;
}

bool CompilerPath::IsValid() const {
	bool expression_path;
	switch (root) {
	case CompilerPathRoot::LOGICAL_PLAN:
		expression_path = false;
		break;
	case CompilerPathRoot::STANDALONE_EXPRESSION:
		expression_path = true;
		break;
	default:
		return false;
	}
	for (auto &component : components) {
		switch (component.type) {
		case CompilerPathComponentType::OPERATOR_CHILD:
			if (expression_path) {
				return false;
			}
			break;
		case CompilerPathComponentType::OPERATOR_EXPRESSION:
			if (expression_path) {
				return false;
			}
			expression_path = true;
			break;
		case CompilerPathComponentType::EXPRESSION_CHILD:
			if (!expression_path) {
				return false;
			}
			break;
		default:
			return false;
		}
	}
	return true;
}

bool CompilerPath::operator==(const CompilerPath &other) const {
	return root == other.root && components == other.components;
}

static uint8_t PathComponentOrder(CompilerPathComponentType type) {
	switch (type) {
	case CompilerPathComponentType::OPERATOR_EXPRESSION:
		return 0;
	case CompilerPathComponentType::OPERATOR_CHILD:
		return 1;
	case CompilerPathComponentType::EXPRESSION_CHILD:
		return 2;
	}
	throw InternalException("Unknown compiler path component type");
}

bool CompilerPath::operator<(const CompilerPath &other) const {
	if (root != other.root) {
		return root < other.root;
	}
	for (idx_t index = 0; index < MinValue(components.size(), other.components.size()); index++) {
		auto &left = components[index];
		auto &right = other.components[index];
		if (left.type != right.type) {
			return PathComponentOrder(left.type) < PathComponentOrder(right.type);
		}
		if (left.ordinal != right.ordinal) {
			return left.ordinal < right.ordinal;
		}
	}
	return components.size() < other.components.size();
}

bool CompilerFunctionIdentity::IsValid() const {
	if (name.empty() || !return_type.IsValid()) {
		return false;
	}
	for (auto &argument : arguments) {
		if (!argument.IsValid()) {
			return false;
		}
	}
	return true;
}

bool CompilerFunctionIdentity::operator==(const CompilerFunctionIdentity &other) const {
	return catalog == other.catalog && schema == other.schema && name == other.name && arguments == other.arguments &&
	       return_type == other.return_type;
}

bool CompilerTypeMismatch::IsValid() const {
	return expected_type.IsValid() && actual_type.IsValid();
}

bool CompilerTypeMismatch::operator==(const CompilerTypeMismatch &other) const {
	return expected_type == other.expected_type && actual_type == other.actual_type;
}

CompilerConstructIdentity CompilerConstructIdentity::LogicalOperator(LogicalOperatorType type) {
	CompilerConstructIdentity result;
	result.type = CompilerConstructType::LOGICAL_OPERATOR;
	result.logical_operator = type;
	return result;
}

CompilerConstructIdentity CompilerConstructIdentity::Expression(ExpressionClass expression_class) {
	CompilerConstructIdentity result;
	result.type = CompilerConstructType::EXPRESSION;
	result.expression = expression_class;
	return result;
}

CompilerConstructIdentity CompilerConstructIdentity::Function(CompilerFunctionIdentity identity) {
	CompilerConstructIdentity result;
	result.type = CompilerConstructType::FUNCTION;
	result.function = std::move(identity);
	return result;
}

CompilerConstructIdentity CompilerConstructIdentity::SourceFunction(CompilerFunctionIdentity identity) {
	CompilerConstructIdentity result;
	result.type = CompilerConstructType::SOURCE_FUNCTION;
	result.function = std::move(identity);
	return result;
}

CompilerConstructIdentity CompilerConstructIdentity::LogicalTypeValue(LogicalType type) {
	CompilerConstructIdentity result;
	result.type = CompilerConstructType::LOGICAL_TYPE;
	result.logical_type = std::move(type);
	return result;
}

CompilerConstructIdentity CompilerConstructIdentity::BindingTypeMismatch(LogicalType expected_type,
                                                                         LogicalType actual_type) {
	CompilerConstructIdentity result;
	result.type = CompilerConstructType::BINDING_TYPE_MISMATCH;
	result.type_mismatch = CompilerTypeMismatch {std::move(expected_type), std::move(actual_type)};
	return result;
}

CompilerConstructIdentity CompilerConstructIdentity::Extension(string identifier) {
	CompilerConstructIdentity result;
	result.type = CompilerConstructType::EXTENSION;
	result.identifier = std::move(identifier);
	return result;
}

CompilerConstructIdentity CompilerConstructIdentity::ExportFeature(string identifier) {
	CompilerConstructIdentity result;
	result.type = CompilerConstructType::EXPORT_FEATURE;
	result.identifier = std::move(identifier);
	return result;
}

bool CompilerConstructIdentity::IsValid() const {
	idx_t payload_count = logical_operator.has_value() + expression.has_value() + function.has_value() +
	                      logical_type.has_value() + type_mismatch.has_value() + identifier.has_value();
	if (payload_count != 1) {
		return false;
	}
	switch (type) {
	case CompilerConstructType::LOGICAL_OPERATOR:
		return logical_operator.has_value() && *logical_operator != LogicalOperatorType::LOGICAL_INVALID;
	case CompilerConstructType::EXPRESSION:
		return expression.has_value() && *expression != ExpressionClass::INVALID;
	case CompilerConstructType::FUNCTION:
	case CompilerConstructType::SOURCE_FUNCTION:
		return function.has_value() && function->IsValid();
	case CompilerConstructType::LOGICAL_TYPE:
		return logical_type.has_value() && logical_type->IsValid();
	case CompilerConstructType::BINDING_TYPE_MISMATCH:
		return type_mismatch.has_value() && type_mismatch->IsValid();
	case CompilerConstructType::EXTENSION:
	case CompilerConstructType::EXPORT_FEATURE:
		return identifier.has_value() && !identifier->empty();
	default:
		return false;
	}
}

bool CompilerConstructIdentity::operator==(const CompilerConstructIdentity &other) const {
	return type == other.type && logical_operator == other.logical_operator && expression == other.expression &&
	       function == other.function && logical_type == other.logical_type && type_mismatch == other.type_mismatch &&
	       identifier == other.identifier;
}

static bool PhaseAllowsIssue(CompilerPhase phase, CompilerIssueCode code) {
	switch (phase) {
	case CompilerPhase::VERIFY: {
		switch (code) {
		case CompilerIssueCode::INVALID_BINDING:
		case CompilerIssueCode::TYPE_MISMATCH:
		case CompilerIssueCode::UNSUPPORTED_OPERATOR:
		case CompilerIssueCode::UNSUPPORTED_EXPRESSION:
		case CompilerIssueCode::UNSUPPORTED_FUNCTION:
		case CompilerIssueCode::UNSUPPORTED_SOURCE:
		case CompilerIssueCode::UNSUPPORTED_EXTENSION:
		case CompilerIssueCode::MALFORMED_EXTENSION_RESULT:
		case CompilerIssueCode::INTERNAL_INVARIANT:
			return true;
		default:
			return false;
		}
	}
	case CompilerPhase::EXPRESSION_EXPORT:
		switch (code) {
		case CompilerIssueCode::INVALID_BINDING:
		case CompilerIssueCode::TYPE_MISMATCH:
		case CompilerIssueCode::UNSUPPORTED_EXPRESSION:
		case CompilerIssueCode::UNSUPPORTED_FUNCTION:
		case CompilerIssueCode::UNSUPPORTED_EXTENSION:
		case CompilerIssueCode::MALFORMED_EXTENSION_RESULT:
		case CompilerIssueCode::UNSUPPORTED_EXPORT_FEATURE:
		case CompilerIssueCode::INTERNAL_INVARIANT:
			return true;
		default:
			return false;
		}
	case CompilerPhase::PLAN_EXPORT: {
		switch (code) {
		case CompilerIssueCode::INVALID_BINDING:
		case CompilerIssueCode::TYPE_MISMATCH:
		case CompilerIssueCode::UNSUPPORTED_OPERATOR:
		case CompilerIssueCode::UNSUPPORTED_EXPRESSION:
		case CompilerIssueCode::UNSUPPORTED_FUNCTION:
		case CompilerIssueCode::UNSUPPORTED_SOURCE:
		case CompilerIssueCode::UNSUPPORTED_EXTENSION:
		case CompilerIssueCode::MALFORMED_EXTENSION_RESULT:
		case CompilerIssueCode::UNSUPPORTED_EXPORT_FEATURE:
		case CompilerIssueCode::INTERNAL_INVARIANT:
			return true;
		default:
			return false;
		}
	}
	default:
		return false;
	}
}

static bool FactsAreValid(const vector<pair<string, Value>> &facts) {
	unordered_set<string> names;
	for (auto &fact : facts) {
		if (fact.first.empty() || fact.second.type().IsNested() || !names.insert(fact.first).second) {
			return false;
		}
	}
	return true;
}

bool CompilerIssue::IsValid() const {
	if (!PhaseAllowsIssue(phase, code) || (path.has_value() && !path->IsValid()) ||
	    (construct.has_value() && !construct->IsValid()) || !FactsAreValid(facts)) {
		return false;
	}
	switch (code) {
	case CompilerIssueCode::INVALID_BINDING:
		return path.has_value();
	case CompilerIssueCode::TYPE_MISMATCH:
		return path.has_value() && construct.has_value() &&
		       construct->type == CompilerConstructType::BINDING_TYPE_MISMATCH;
	case CompilerIssueCode::UNSUPPORTED_OPERATOR:
		return path.has_value() && construct.has_value() &&
		       construct->type == CompilerConstructType::LOGICAL_OPERATOR &&
		       *construct->logical_operator != LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR;
	case CompilerIssueCode::UNSUPPORTED_EXPRESSION:
		return path.has_value() && construct.has_value() && construct->type == CompilerConstructType::EXPRESSION;
	case CompilerIssueCode::UNSUPPORTED_FUNCTION:
		return path.has_value() && construct.has_value() && construct->type == CompilerConstructType::FUNCTION;
	case CompilerIssueCode::UNSUPPORTED_SOURCE:
		return path.has_value() && construct.has_value() && construct->type == CompilerConstructType::SOURCE_FUNCTION;
	case CompilerIssueCode::UNSUPPORTED_EXTENSION:
		return path.has_value() && construct.has_value() && construct->type == CompilerConstructType::EXTENSION;
	case CompilerIssueCode::MALFORMED_EXTENSION_RESULT:
		return construct.has_value() && construct->type == CompilerConstructType::EXTENSION;
	case CompilerIssueCode::UNSUPPORTED_EXPORT_FEATURE:
		return path.has_value() && construct.has_value() &&
		       construct->type != CompilerConstructType::BINDING_TYPE_MISMATCH;
	case CompilerIssueCode::INTERNAL_INVARIANT:
		return true;
	default:
		return false;
	}
}

bool CompilerIssue::operator==(const CompilerIssue &other) const {
	return code == other.code && phase == other.phase && path == other.path && construct == other.construct &&
	       facts == other.facts;
}

bool CompilerIssue::operator<(const CompilerIssue &other) const {
	if (!(path == other.path)) {
		if (!path.has_value()) {
			return true;
		}
		if (!other.path.has_value()) {
			return false;
		}
		return *path < *other.path;
	}
	if (phase != other.phase) {
		return phase < other.phase;
	}
	return code < other.code;
}

} // namespace duckdb
