#include "duckdb/planner/logical_plan_compiler_result.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {

bool LogicalPlanCompilerPathComponent::operator==(const LogicalPlanCompilerPathComponent &other) const {
	return type == other.type && ordinal == other.ordinal;
}

bool LogicalPlanCompilerPath::IsValid() const {
	bool expression_path;
	switch (root) {
	case LogicalPlanCompilerPathRoot::LOGICAL_PLAN:
		expression_path = false;
		break;
	case LogicalPlanCompilerPathRoot::STANDALONE_EXPRESSION:
		expression_path = true;
		break;
	default:
		return false;
	}
	for (auto &component : components) {
		switch (component.type) {
		case LogicalPlanCompilerPathComponentType::OPERATOR_CHILD:
			if (expression_path) {
				return false;
			}
			break;
		case LogicalPlanCompilerPathComponentType::OPERATOR_EXPRESSION:
			if (expression_path) {
				return false;
			}
			expression_path = true;
			break;
		case LogicalPlanCompilerPathComponentType::EXPRESSION_CHILD:
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

bool LogicalPlanCompilerPath::operator==(const LogicalPlanCompilerPath &other) const {
	return root == other.root && components == other.components;
}

static uint8_t PathComponentOrder(LogicalPlanCompilerPathComponentType type) {
	switch (type) {
	case LogicalPlanCompilerPathComponentType::OPERATOR_EXPRESSION:
		return 0;
	case LogicalPlanCompilerPathComponentType::OPERATOR_CHILD:
		return 1;
	case LogicalPlanCompilerPathComponentType::EXPRESSION_CHILD:
		return 2;
	}
	throw InternalException("Unknown compiler path component type");
}

bool LogicalPlanCompilerPath::operator<(const LogicalPlanCompilerPath &other) const {
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

bool LogicalPlanCompilerFunctionIdentity::IsValid() const {
	if (name.empty() || !return_type.IsComplete()) {
		return false;
	}
	for (auto &argument : arguments) {
		if (!argument.IsComplete()) {
			return false;
		}
	}
	return true;
}

bool LogicalPlanCompilerFunctionIdentity::operator==(const LogicalPlanCompilerFunctionIdentity &other) const {
	return catalog == other.catalog && schema == other.schema && name == other.name && arguments == other.arguments &&
	       return_type == other.return_type;
}

bool LogicalPlanCompilerTypeMismatch::IsValid() const {
	return expected_type.IsComplete() && actual_type.IsComplete();
}

bool LogicalPlanCompilerTypeMismatch::operator==(const LogicalPlanCompilerTypeMismatch &other) const {
	return expected_type == other.expected_type && actual_type == other.actual_type;
}

LogicalPlanCompilerConstructIdentity LogicalPlanCompilerConstructIdentity::LogicalOperator(LogicalOperatorType type) {
	LogicalPlanCompilerConstructIdentity result;
	result.type = LogicalPlanCompilerConstructType::LOGICAL_OPERATOR;
	result.logical_operator = type;
	return result;
}

LogicalPlanCompilerConstructIdentity
LogicalPlanCompilerConstructIdentity::Expression(ExpressionClass expression_class) {
	LogicalPlanCompilerConstructIdentity result;
	result.type = LogicalPlanCompilerConstructType::EXPRESSION;
	result.expression = expression_class;
	return result;
}

LogicalPlanCompilerConstructIdentity
LogicalPlanCompilerConstructIdentity::Function(LogicalPlanCompilerFunctionIdentity identity) {
	LogicalPlanCompilerConstructIdentity result;
	result.type = LogicalPlanCompilerConstructType::FUNCTION;
	result.function = std::move(identity);
	return result;
}

LogicalPlanCompilerConstructIdentity
LogicalPlanCompilerConstructIdentity::SourceFunction(LogicalPlanCompilerFunctionIdentity identity) {
	LogicalPlanCompilerConstructIdentity result;
	result.type = LogicalPlanCompilerConstructType::SOURCE_FUNCTION;
	result.function = std::move(identity);
	return result;
}

LogicalPlanCompilerConstructIdentity LogicalPlanCompilerConstructIdentity::LogicalTypeValue(LogicalType type) {
	LogicalPlanCompilerConstructIdentity result;
	result.type = LogicalPlanCompilerConstructType::LOGICAL_TYPE;
	result.logical_type = std::move(type);
	return result;
}

LogicalPlanCompilerConstructIdentity
LogicalPlanCompilerConstructIdentity::BindingTypeMismatch(LogicalType expected_type, LogicalType actual_type) {
	LogicalPlanCompilerConstructIdentity result;
	result.type = LogicalPlanCompilerConstructType::BINDING_TYPE_MISMATCH;
	result.type_mismatch = LogicalPlanCompilerTypeMismatch {std::move(expected_type), std::move(actual_type)};
	return result;
}

LogicalPlanCompilerConstructIdentity LogicalPlanCompilerConstructIdentity::Extension(string identifier) {
	LogicalPlanCompilerConstructIdentity result;
	result.type = LogicalPlanCompilerConstructType::EXTENSION;
	result.identifier = std::move(identifier);
	return result;
}

LogicalPlanCompilerConstructIdentity LogicalPlanCompilerConstructIdentity::ExportFeature(string identifier) {
	LogicalPlanCompilerConstructIdentity result;
	result.type = LogicalPlanCompilerConstructType::EXPORT_FEATURE;
	result.identifier = std::move(identifier);
	return result;
}

bool LogicalPlanCompilerConstructIdentity::IsValid() const {
	idx_t payload_count = logical_operator.has_value() + expression.has_value() + function.has_value() +
	                      logical_type.has_value() + type_mismatch.has_value() + identifier.has_value();
	if (payload_count != 1) {
		return false;
	}
	switch (type) {
	case LogicalPlanCompilerConstructType::LOGICAL_OPERATOR:
		return logical_operator.has_value() && *logical_operator != LogicalOperatorType::LOGICAL_INVALID;
	case LogicalPlanCompilerConstructType::EXPRESSION:
		return expression.has_value() && *expression != ExpressionClass::INVALID;
	case LogicalPlanCompilerConstructType::FUNCTION:
	case LogicalPlanCompilerConstructType::SOURCE_FUNCTION:
		return function.has_value() && function->IsValid();
	case LogicalPlanCompilerConstructType::LOGICAL_TYPE:
		return logical_type.has_value() && logical_type->IsComplete();
	case LogicalPlanCompilerConstructType::BINDING_TYPE_MISMATCH:
		return type_mismatch.has_value() && type_mismatch->IsValid();
	case LogicalPlanCompilerConstructType::EXTENSION:
	case LogicalPlanCompilerConstructType::EXPORT_FEATURE:
		return identifier.has_value() && !identifier->empty();
	default:
		return false;
	}
}

bool LogicalPlanCompilerConstructIdentity::operator==(const LogicalPlanCompilerConstructIdentity &other) const {
	return type == other.type && logical_operator == other.logical_operator && expression == other.expression &&
	       function == other.function && logical_type == other.logical_type && type_mismatch == other.type_mismatch &&
	       identifier == other.identifier;
}

template <class T>
static bool SerializedLess(const T &left, const T &right) {
	MemoryStream left_stream;
	MemoryStream right_stream;
	BinarySerializer::Serialize(left, left_stream);
	BinarySerializer::Serialize(right, right_stream);
	auto compare_count = MinValue(left_stream.GetPosition(), right_stream.GetPosition());
	for (idx_t index = 0; index < compare_count; index++) {
		if (left_stream.GetData()[index] != right_stream.GetData()[index]) {
			return left_stream.GetData()[index] < right_stream.GetData()[index];
		}
	}
	return left_stream.GetPosition() < right_stream.GetPosition();
}

template <class T>
static bool SerializedEquals(const T &left, const T &right) {
	MemoryStream left_stream;
	MemoryStream right_stream;
	BinarySerializer::Serialize(left, left_stream);
	BinarySerializer::Serialize(right, right_stream);
	if (left_stream.GetPosition() != right_stream.GetPosition()) {
		return false;
	}
	for (idx_t index = 0; index < left_stream.GetPosition(); index++) {
		if (left_stream.GetData()[index] != right_stream.GetData()[index]) {
			return false;
		}
	}
	return true;
}

static bool FunctionIdentityLess(const LogicalPlanCompilerFunctionIdentity &left,
                                 const LogicalPlanCompilerFunctionIdentity &right) {
	if (left.catalog != right.catalog) {
		return left.catalog < right.catalog;
	}
	if (left.schema != right.schema) {
		return left.schema < right.schema;
	}
	if (left.name != right.name) {
		return left.name < right.name;
	}
	for (idx_t index = 0; index < MinValue(left.arguments.size(), right.arguments.size()); index++) {
		if (left.arguments[index] != right.arguments[index]) {
			return SerializedLess(left.arguments[index], right.arguments[index]);
		}
	}
	if (left.arguments.size() != right.arguments.size()) {
		return left.arguments.size() < right.arguments.size();
	}
	if (left.return_type != right.return_type) {
		return SerializedLess(left.return_type, right.return_type);
	}
	return false;
}

static bool TypeMismatchLess(const LogicalPlanCompilerTypeMismatch &left,
                             const LogicalPlanCompilerTypeMismatch &right) {
	if (left.expected_type != right.expected_type) {
		return SerializedLess(left.expected_type, right.expected_type);
	}
	if (left.actual_type != right.actual_type) {
		return SerializedLess(left.actual_type, right.actual_type);
	}
	return false;
}

bool LogicalPlanCompilerConstructIdentity::operator<(const LogicalPlanCompilerConstructIdentity &other) const {
	if (type != other.type) {
		return type < other.type;
	}
	switch (type) {
	case LogicalPlanCompilerConstructType::LOGICAL_OPERATOR:
		return logical_operator < other.logical_operator;
	case LogicalPlanCompilerConstructType::EXPRESSION:
		return expression < other.expression;
	case LogicalPlanCompilerConstructType::FUNCTION:
	case LogicalPlanCompilerConstructType::SOURCE_FUNCTION:
		if (function == other.function) {
			return false;
		}
		if (!function.has_value()) {
			return true;
		}
		if (!other.function.has_value()) {
			return false;
		}
		return FunctionIdentityLess(*function, *other.function);
	case LogicalPlanCompilerConstructType::LOGICAL_TYPE:
		if (logical_type == other.logical_type) {
			return false;
		}
		if (!logical_type.has_value()) {
			return true;
		}
		if (!other.logical_type.has_value()) {
			return false;
		}
		return SerializedLess(*logical_type, *other.logical_type);
	case LogicalPlanCompilerConstructType::BINDING_TYPE_MISMATCH:
		if (type_mismatch == other.type_mismatch) {
			return false;
		}
		if (!type_mismatch.has_value()) {
			return true;
		}
		if (!other.type_mismatch.has_value()) {
			return false;
		}
		return TypeMismatchLess(*type_mismatch, *other.type_mismatch);
	case LogicalPlanCompilerConstructType::EXTENSION:
	case LogicalPlanCompilerConstructType::EXPORT_FEATURE:
		return identifier < other.identifier;
	default:
		return false;
	}
}

static bool PhaseAllowsIssue(LogicalPlanCompilerPhase phase, LogicalPlanCompilerIssueCode code) {
	switch (phase) {
	case LogicalPlanCompilerPhase::VERIFY: {
		switch (code) {
		case LogicalPlanCompilerIssueCode::INVALID_BINDING:
		case LogicalPlanCompilerIssueCode::TYPE_MISMATCH:
		case LogicalPlanCompilerIssueCode::UNSUPPORTED_OPERATOR:
		case LogicalPlanCompilerIssueCode::UNSUPPORTED_EXPRESSION:
		case LogicalPlanCompilerIssueCode::UNSUPPORTED_FUNCTION:
		case LogicalPlanCompilerIssueCode::UNSUPPORTED_SOURCE:
		case LogicalPlanCompilerIssueCode::UNSUPPORTED_EXTENSION:
		case LogicalPlanCompilerIssueCode::MALFORMED_EXTENSION_RESULT:
		case LogicalPlanCompilerIssueCode::INTERNAL_INVARIANT:
			return true;
		default:
			return false;
		}
	}
	case LogicalPlanCompilerPhase::EXPRESSION_EXPORT:
		switch (code) {
		case LogicalPlanCompilerIssueCode::INVALID_BINDING:
		case LogicalPlanCompilerIssueCode::TYPE_MISMATCH:
		case LogicalPlanCompilerIssueCode::UNSUPPORTED_EXPRESSION:
		case LogicalPlanCompilerIssueCode::UNSUPPORTED_FUNCTION:
		case LogicalPlanCompilerIssueCode::UNSUPPORTED_EXTENSION:
		case LogicalPlanCompilerIssueCode::MALFORMED_EXTENSION_RESULT:
		case LogicalPlanCompilerIssueCode::UNSUPPORTED_EXPORT_FEATURE:
		case LogicalPlanCompilerIssueCode::INTERNAL_INVARIANT:
			return true;
		default:
			return false;
		}
	case LogicalPlanCompilerPhase::PLAN_EXPORT: {
		switch (code) {
		case LogicalPlanCompilerIssueCode::INVALID_BINDING:
		case LogicalPlanCompilerIssueCode::TYPE_MISMATCH:
		case LogicalPlanCompilerIssueCode::UNSUPPORTED_OPERATOR:
		case LogicalPlanCompilerIssueCode::UNSUPPORTED_EXPRESSION:
		case LogicalPlanCompilerIssueCode::UNSUPPORTED_FUNCTION:
		case LogicalPlanCompilerIssueCode::UNSUPPORTED_SOURCE:
		case LogicalPlanCompilerIssueCode::UNSUPPORTED_EXTENSION:
		case LogicalPlanCompilerIssueCode::MALFORMED_EXTENSION_RESULT:
		case LogicalPlanCompilerIssueCode::UNSUPPORTED_EXPORT_FEATURE:
		case LogicalPlanCompilerIssueCode::INTERNAL_INVARIANT:
			return true;
		default:
			return false;
		}
	}
	default:
		return false;
	}
}

static bool IsStableFactType(const LogicalType &type) {
	if (!type.IsComplete()) {
		return false;
	}
	switch (type.id()) {
	case LogicalTypeId::SQLNULL:
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::CHAR:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::INTERVAL:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_TZ_NS:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::TIME_NS:
	case LogicalTypeId::BIT:
	case LogicalTypeId::BIGNUM:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UUID:
	case LogicalTypeId::GEOMETRY:
	case LogicalTypeId::ENUM:
		return true;
	default:
		return false;
	}
}

static bool FactsAreValid(const vector<pair<string, Value>> &facts) {
	unordered_set<string> names;
	for (auto &fact : facts) {
		if (fact.first.empty() || !IsStableFactType(fact.second.type()) || !names.insert(fact.first).second) {
			return false;
		}
	}
	return true;
}

static bool FactLess(const pair<string, Value> &left, const pair<string, Value> &right) {
	if (left.first != right.first) {
		return left.first < right.first;
	}
	return SerializedLess(left.second, right.second);
}

static bool FactEquals(const pair<string, Value> &left, const pair<string, Value> &right) {
	return left.first == right.first && SerializedEquals(left.second, right.second);
}

void LogicalPlanCompilerIssue::Normalize() {
	std::sort(facts.begin(), facts.end(), FactLess);
}

bool LogicalPlanCompilerIssue::IsValid() const {
	if (!PhaseAllowsIssue(phase, code) || (path.has_value() && !path->IsValid()) ||
	    (construct.has_value() && !construct->IsValid()) || !FactsAreValid(facts)) {
		return false;
	}
	switch (code) {
	case LogicalPlanCompilerIssueCode::INVALID_BINDING:
		return path.has_value();
	case LogicalPlanCompilerIssueCode::TYPE_MISMATCH:
		return path.has_value() && construct.has_value() &&
		       construct->type == LogicalPlanCompilerConstructType::BINDING_TYPE_MISMATCH;
	case LogicalPlanCompilerIssueCode::UNSUPPORTED_OPERATOR:
		return path.has_value() && construct.has_value() &&
		       construct->type == LogicalPlanCompilerConstructType::LOGICAL_OPERATOR &&
		       *construct->logical_operator != LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR;
	case LogicalPlanCompilerIssueCode::UNSUPPORTED_EXPRESSION:
		return path.has_value() && construct.has_value() &&
		       construct->type == LogicalPlanCompilerConstructType::EXPRESSION;
	case LogicalPlanCompilerIssueCode::UNSUPPORTED_FUNCTION:
		return path.has_value() && construct.has_value() &&
		       construct->type == LogicalPlanCompilerConstructType::FUNCTION;
	case LogicalPlanCompilerIssueCode::UNSUPPORTED_SOURCE:
		return path.has_value() && construct.has_value() &&
		       construct->type == LogicalPlanCompilerConstructType::SOURCE_FUNCTION;
	case LogicalPlanCompilerIssueCode::UNSUPPORTED_EXTENSION:
		return path.has_value() && construct.has_value() &&
		       construct->type == LogicalPlanCompilerConstructType::EXTENSION;
	case LogicalPlanCompilerIssueCode::MALFORMED_EXTENSION_RESULT:
		return construct.has_value() && construct->type == LogicalPlanCompilerConstructType::EXTENSION;
	case LogicalPlanCompilerIssueCode::UNSUPPORTED_EXPORT_FEATURE:
		return path.has_value() && construct.has_value() &&
		       construct->type != LogicalPlanCompilerConstructType::BINDING_TYPE_MISMATCH;
	case LogicalPlanCompilerIssueCode::INTERNAL_INVARIANT:
		return true;
	default:
		return false;
	}
}

bool LogicalPlanCompilerIssue::operator==(const LogicalPlanCompilerIssue &other) const {
	if (code != other.code || phase != other.phase || !(path == other.path) || !(construct == other.construct) ||
	    facts.size() != other.facts.size()) {
		return false;
	}
	for (idx_t fact_index = 0; fact_index < facts.size(); fact_index++) {
		if (!FactEquals(facts[fact_index], other.facts[fact_index])) {
			return false;
		}
	}
	return true;
}

bool LogicalPlanCompilerIssue::operator<(const LogicalPlanCompilerIssue &other) const {
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
	if (code != other.code) {
		return code < other.code;
	}
	if (!(construct == other.construct)) {
		if (!construct.has_value()) {
			return true;
		}
		if (!other.construct.has_value()) {
			return false;
		}
		return *construct < *other.construct;
	}
	return std::lexicographical_compare(facts.begin(), facts.end(), other.facts.begin(), other.facts.end(), FactLess);
}

} // namespace duckdb
