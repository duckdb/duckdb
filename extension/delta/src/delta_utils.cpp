#include "delta_utils.hpp"

#include "duckdb.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

namespace duckdb {

unique_ptr<SchemaVisitor::FieldList> SchemaVisitor::VisitSnapshotSchema(ffi::SharedSnapshot *snapshot) {
	SchemaVisitor state;
	ffi::EngineSchemaVisitor visitor;

	visitor.data = &state;
	visitor.make_field_list = (uintptr_t(*)(void *, uintptr_t)) & MakeFieldList;
	visitor.visit_struct = (void (*)(void *, uintptr_t, ffi::KernelStringSlice, uintptr_t)) & VisitStruct;
	visitor.visit_array = (void (*)(void *, uintptr_t, ffi::KernelStringSlice, bool, uintptr_t)) & VisitArray;
	visitor.visit_map = (void (*)(void *, uintptr_t, ffi::KernelStringSlice, bool, uintptr_t)) & VisitMap;
	visitor.visit_decimal = (void (*)(void *, uintptr_t, ffi::KernelStringSlice, uint8_t, uint8_t)) & VisitDecimal;
	visitor.visit_string = VisitSimpleType<LogicalType::VARCHAR>();
	visitor.visit_long = VisitSimpleType<LogicalType::BIGINT>();
	visitor.visit_integer = VisitSimpleType<LogicalType::INTEGER>();
	visitor.visit_short = VisitSimpleType<LogicalType::SMALLINT>();
	visitor.visit_byte = VisitSimpleType<LogicalType::TINYINT>();
	visitor.visit_float = VisitSimpleType<LogicalType::FLOAT>();
	visitor.visit_double = VisitSimpleType<LogicalType::DOUBLE>();
	visitor.visit_boolean = VisitSimpleType<LogicalType::BOOLEAN>();
	visitor.visit_binary = VisitSimpleType<LogicalType::VARCHAR>();
	visitor.visit_date = VisitSimpleType<LogicalType::DATE>();
	visitor.visit_timestamp = VisitSimpleType<LogicalType::TIMESTAMP>();
	visitor.visit_timestamp_ntz = VisitSimpleType<LogicalType::TIMESTAMP_TZ>();

	uintptr_t result = visit_schema(snapshot, &visitor);
	return state.TakeFieldList(result);
}

void SchemaVisitor::VisitDecimal(SchemaVisitor *state, uintptr_t sibling_list_id, ffi::KernelStringSlice name,
                                 uint8_t precision, uint8_t scale) {
	state->AppendToList(sibling_list_id, name, LogicalType::DECIMAL(precision, scale));
}

uintptr_t SchemaVisitor::MakeFieldList(SchemaVisitor *state, uintptr_t capacity_hint) {
	return state->MakeFieldListImpl(capacity_hint);
}

void SchemaVisitor::VisitStruct(SchemaVisitor *state, uintptr_t sibling_list_id, ffi::KernelStringSlice name,
                                uintptr_t child_list_id) {
	auto children = state->TakeFieldList(child_list_id);
	state->AppendToList(sibling_list_id, name, LogicalType::STRUCT(std::move(*children)));
}

void SchemaVisitor::VisitArray(SchemaVisitor *state, uintptr_t sibling_list_id, ffi::KernelStringSlice name,
                               bool contains_null, uintptr_t child_list_id) {
	auto children = state->TakeFieldList(child_list_id);

	D_ASSERT(children->size() == 1);
	state->AppendToList(sibling_list_id, name, LogicalType::LIST(children->front().second));
}

void SchemaVisitor::VisitMap(SchemaVisitor *state, uintptr_t sibling_list_id, ffi::KernelStringSlice name,
                             bool contains_null, uintptr_t child_list_id) {
	auto children = state->TakeFieldList(child_list_id);

	D_ASSERT(children->size() == 2);
	state->AppendToList(sibling_list_id, name, LogicalType::MAP(LogicalType::STRUCT(std::move(*children))));
}

uintptr_t SchemaVisitor::MakeFieldListImpl(uintptr_t capacity_hint) {
	uintptr_t id = next_id++;
	auto list = make_uniq<FieldList>();
	if (capacity_hint > 0) {
		list->reserve(capacity_hint);
	}
	inflight_lists.emplace(id, std::move(list));
	return id;
}

void SchemaVisitor::AppendToList(uintptr_t id, ffi::KernelStringSlice name, LogicalType &&child) {
	auto it = inflight_lists.find(id);
	if (it == inflight_lists.end()) {
		// TODO... some error...
		throw InternalException("WEIRD SHIT");
	} else {
		it->second->emplace_back(std::make_pair(string(name.ptr, name.len), std::move(child)));
	}
}

unique_ptr<SchemaVisitor::FieldList> SchemaVisitor::TakeFieldList(uintptr_t id) {
	auto it = inflight_lists.find(id);
	if (it == inflight_lists.end()) {
		// TODO: Raise some kind of error.
		throw InternalException("WEIRD SHIT 2");
	}
	auto rval = std::move(it->second);
	inflight_lists.erase(it);
	return rval;
}

ffi::EngineError *DuckDBEngineError::AllocateError(ffi::KernelError etype, ffi::KernelStringSlice msg) {
	auto error = new DuckDBEngineError;
	error->etype = etype;
	error->error_message = string(msg.ptr, msg.len);
	return error;
}

string DuckDBEngineError::KernelErrorEnumToString(ffi::KernelError err) {
	const char *KERNEL_ERROR_ENUM_STRINGS[] = {
	    "UnknownError",
	    "FFIError",
	    "ArrowError",
	    "EngineDataTypeError",
	    "ExtractError",
	    "GenericError",
	    "IOErrorError",
	    "ParquetError",
	    "ObjectStoreError",
	    "ObjectStorePathError",
	    "Reqwest",
	    "FileNotFoundError",
	    "MissingColumnError",
	    "UnexpectedColumnTypeError",
	    "MissingDataError",
	    "MissingVersionError",
	    "DeletionVectorError",
	    "InvalidUrlError",
	    "MalformedJsonError",
	    "MissingMetadataError",
	    "MissingProtocolError",
	    "MissingMetadataAndProtocolError",
	    "ParseError",
	    "JoinFailureError",
	    "Utf8Error",
	    "ParseIntError",
	    "InvalidColumnMappingMode",
	    "InvalidTableLocation",
	    "InvalidDecimalError",
	};

	static_assert(sizeof(KERNEL_ERROR_ENUM_STRINGS) / sizeof(char *) - 1 == (int)ffi::KernelError::InvalidDecimalError,
	              "KernelErrorEnumStrings mismatched with kernel");

	if ((int)err < sizeof(KERNEL_ERROR_ENUM_STRINGS) / sizeof(char *)) {
		return KERNEL_ERROR_ENUM_STRINGS[(int)err];
	}

	return StringUtil::Format("EnumOutOfRange (enum val out of range: %d)", (int)err);
}

void DuckDBEngineError::Throw(string from_where) {
	// Make copies before calling delete this
	auto etype_copy = etype;
	auto message_copy = error_message;

	// Consume error by calling delete this (remember this error is created by kernel using AllocateError)
	delete this;
	throw IOException("Hit DeltaKernel FFI error (from: %s): Hit error: %u (%s) with message (%s)", from_where.c_str(),
	                  etype_copy, KernelErrorEnumToString(etype_copy), message_copy);
}

ffi::KernelStringSlice KernelUtils::ToDeltaString(const string &str) {
	return {str.data(), str.size()};
}

string KernelUtils::FromDeltaString(const struct ffi::KernelStringSlice slice) {
	return {slice.ptr, slice.len};
}

vector<bool> KernelUtils::FromDeltaBoolSlice(const struct ffi::KernelBoolSlice slice) {
	vector<bool> result;
	result.assign(slice.ptr, slice.ptr + slice.len);
	return result;
}

PredicateVisitor::PredicateVisitor(const vector<string> &column_names, optional_ptr<TableFilterSet> filters)
    : EnginePredicate {.predicate = this,
                       .visitor = (uintptr_t(*)(void *, ffi::KernelExpressionVisitorState *)) & VisitPredicate} {
	if (filters) {
		for (auto &filter : filters->filters) {
			column_filters[column_names[filter.first]] = filter.second.get();
		}
	}
}

// Template wrapper function that implements get_next for EngineIteratorFromCallable.
template <typename Callable>
static auto GetNextFromCallable(Callable *callable) -> decltype(std::declval<Callable>()()) {
	return callable->operator()();
}

// Wraps a callable object (e.g. C++11 lambda) as an EngineIterator.
template <typename Callable>
ffi::EngineIterator EngineIteratorFromCallable(Callable &callable) {
	auto *get_next = &GetNextFromCallable<Callable>;
	return {.data = &callable, .get_next = (const void *(*)(void *))get_next};
};

// Helper function to prevent pushing down filters kernel cant handle
// TODO: remove once kernel handles this properly?
static bool CanHandleFilter(TableFilter *filter) {
	auto &expr_filter = ExpressionFilter::GetExpressionFilter(*filter, "DeltaUtils::CanHandleFilter");
	auto &expr = *expr_filter.expr;
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
		auto &comparison = expr.Cast<BoundComparisonExpression>();
		auto left_class = comparison.left->GetExpressionClass();
		auto right_class = comparison.right->GetExpressionClass();
		auto left_is_ref = left_class == ExpressionClass::BOUND_REF || left_class == ExpressionClass::BOUND_COLUMN_REF;
		auto right_is_ref =
		    right_class == ExpressionClass::BOUND_REF || right_class == ExpressionClass::BOUND_COLUMN_REF;
		auto left_is_constant = comparison.left->type == ExpressionType::VALUE_CONSTANT;
		auto right_is_constant = comparison.right->type == ExpressionType::VALUE_CONSTANT;
		return (left_is_ref && right_is_constant) || (left_is_constant && right_is_ref);
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION &&
	    expr.type == ExpressionType::CONJUNCTION_AND) {
		auto &conjunction = expr.Cast<BoundConjunctionExpression>();
		for (const auto &child : conjunction.children) {
			auto child_filter = ExpressionFilter(child->Copy());
			if (!CanHandleFilter(&child_filter)) {
				return false;
			}
		}
		return true;
	}
	return false;
}

// Prunes the list of predicates to ones that we can handle
static unordered_map<string, TableFilter *> PrunePredicates(unordered_map<string, TableFilter *> predicates) {
	unordered_map<string, TableFilter *> result;
	for (const auto &predicate : predicates) {
		if (CanHandleFilter(predicate.second)) {
			result[predicate.first] = predicate.second;
		}
	}
	return result;
}

uintptr_t PredicateVisitor::VisitPredicate(PredicateVisitor *predicate, ffi::KernelExpressionVisitorState *state) {
	auto filters = PrunePredicates(predicate->column_filters);

	auto it = filters.begin();
	auto end = filters.end();
	auto get_next = [predicate, state, &it, &end]() -> uintptr_t {
		if (it == end) {
			return 0;
		}
		auto &filter = *it++;
		return predicate->VisitFilter(filter.first, *filter.second, state);
	};
	auto eit = EngineIteratorFromCallable(get_next);

	// TODO: this should be fixed upstream?
	try {
		return visit_expression_and(state, &eit);
	} catch (...) {
		return ~0;
	}
}

uintptr_t PredicateVisitor::VisitComparisonExpression(const string &col_name, const BoundComparisonExpression &expr,
                                                      ffi::KernelExpressionVisitorState *state) {
	auto maybe_left =
	    ffi::visit_expression_column(state, KernelUtils::ToDeltaString(col_name), DuckDBEngineError::AllocateError);
	uintptr_t left = KernelUtils::UnpackResult(maybe_left, "VisitConstantFilter failed to visit_expression_column");

	uintptr_t right = ~0;
	auto comparison_type = expr.type;
	optional_ptr<Expression> constant_expr;
	if (expr.right->type == ExpressionType::VALUE_CONSTANT) {
		constant_expr = expr.right.get();
	} else if (expr.left->type == ExpressionType::VALUE_CONSTANT) {
		constant_expr = expr.left.get();
		comparison_type = FlipComparisonExpression(comparison_type);
	} else {
		return ~0;
	}
	auto &value = constant_expr->Cast<BoundConstantExpression>().value;
	switch (value.type().id()) {
	case LogicalType::BIGINT:
		right = visit_expression_literal_long(state, BigIntValue::Get(value));
		break;

	case LogicalType::VARCHAR: {
		// WARNING: C++ lifetime extension rules don't protect calls of the form foo(std::string(...).c_str())
		auto str = StringValue::Get(value);
		auto maybe_right = ffi::visit_expression_literal_string(state, KernelUtils::ToDeltaString(str),
		                                                        DuckDBEngineError::AllocateError);
		right = KernelUtils::UnpackResult(maybe_right, "VisitConstantFilter failed to visit_expression_literal_string");
		break;
	}

	default:
		break; // unsupported type
	}

	// TODO support other comparison types?
	switch (comparison_type) {
	case ExpressionType::COMPARE_LESSTHAN:
		return visit_expression_lt(state, left, right);
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return visit_expression_le(state, left, right);
	case ExpressionType::COMPARE_GREATERTHAN:
		return visit_expression_gt(state, left, right);
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return visit_expression_ge(state, left, right);
	case ExpressionType::COMPARE_EQUAL:
		return visit_expression_eq(state, left, right);

	default:
		std::cout << " Unsupported operation: " << (int)comparison_type << std::endl;
		return ~0; // Unsupported operation
	}
}

uintptr_t PredicateVisitor::VisitAndExpression(const string &col_name, const BoundConjunctionExpression &expr,
                                               ffi::KernelExpressionVisitorState *state) {
	auto it = expr.children.begin();
	auto end = expr.children.end();
	auto get_next = [this, col_name, state, &it, &end]() -> uintptr_t {
		if (it == end) {
			return 0;
		}
		auto &child_expr = **it++;
		return VisitExpression(col_name, child_expr, state);
	};
	auto eit = EngineIteratorFromCallable(get_next);
	return visit_expression_and(state, &eit);
}

uintptr_t PredicateVisitor::VisitExpression(const string &col_name, const Expression &expr,
                                            ffi::KernelExpressionVisitorState *state) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_COMPARISON:
		return VisitComparisonExpression(col_name, expr.Cast<BoundComparisonExpression>(), state);
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conjunction = expr.Cast<BoundConjunctionExpression>();
		if (expr.type == ExpressionType::CONJUNCTION_AND) {
			return VisitAndExpression(col_name, conjunction, state);
		}
		break;
	}
	default:
		break;
	}
	throw NotImplementedException("Attempted to push down unimplemented filter expression: '%s'", expr.ToString());
}

uintptr_t PredicateVisitor::VisitFilter(const string &col_name, const TableFilter &filter,
                                        ffi::KernelExpressionVisitorState *state) {
	auto &expr_filter = ExpressionFilter::GetExpressionFilter(filter, "PredicateVisitor::VisitFilter");
	return VisitExpression(col_name, *expr_filter.expr, state);
}

}; // namespace duckdb
