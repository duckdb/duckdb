#include "duckdb.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/common/column_index.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/planner/extension_callback.hpp"
#include "duckdb/planner/planner_extension.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/common/extension_type_info.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"

using namespace duckdb;

//===--------------------------------------------------------------------===//
// Scalar function
//===--------------------------------------------------------------------===//
static inline int32_t hello_fun(string_t what) {
	return what.GetSize() + 5;
}

static inline void TestAliasHello(DataChunk &args, ExpressionState &state, Vector &result) {
	result.Reference(Value("Hello Alias!"));
}

static inline void AddPointFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &left_vector = args.data[0];
	auto &right_vector = args.data[1];
	const int count = args.size();
	auto left_vector_type = left_vector.GetVectorType();
	auto right_vector_type = right_vector.GetVectorType();

	args.Flatten();

	UnifiedVectorFormat lhs_data;
	UnifiedVectorFormat rhs_data;
	left_vector.ToUnifiedFormat(count, lhs_data);
	right_vector.ToUnifiedFormat(count, rhs_data);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto &child_entries = StructVector::GetEntries(result);
	auto &left_child_entries = StructVector::GetEntries(left_vector);
	auto &right_child_entries = StructVector::GetEntries(right_vector);
	for (int base_idx = 0; base_idx < count; base_idx++) {
		auto lhs_list_index = lhs_data.sel->get_index(base_idx);
		auto rhs_list_index = rhs_data.sel->get_index(base_idx);
		if (!lhs_data.validity.RowIsValid(lhs_list_index) || !rhs_data.validity.RowIsValid(rhs_list_index)) {
			FlatVector::SetNull(result, base_idx, true);
			continue;
		}
		for (size_t col = 0; col < child_entries.size(); ++col) {
			auto &child_entry = child_entries[col];
			auto &left_child_entry = left_child_entries[col];
			auto &right_child_entry = right_child_entries[col];
			auto pdata = ConstantVector::GetData<int32_t>(*child_entry);
			auto left_pdata = ConstantVector::GetData<int32_t>(*left_child_entry);
			auto right_pdata = ConstantVector::GetData<int32_t>(*right_child_entry);
			pdata[base_idx] = left_pdata[lhs_list_index] + right_pdata[rhs_list_index];
		}
	}
	if (left_vector_type == VectorType::CONSTANT_VECTOR && right_vector_type == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	result.Verify(count);
}

static inline void SubPointFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &left_vector = args.data[0];
	auto &right_vector = args.data[1];
	const int count = args.size();
	auto left_vector_type = left_vector.GetVectorType();
	auto right_vector_type = right_vector.GetVectorType();

	args.Flatten();
	UnifiedVectorFormat lhs_data;
	UnifiedVectorFormat rhs_data;
	left_vector.ToUnifiedFormat(count, lhs_data);
	right_vector.ToUnifiedFormat(count, rhs_data);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto &child_entries = StructVector::GetEntries(result);
	auto &left_child_entries = StructVector::GetEntries(left_vector);
	auto &right_child_entries = StructVector::GetEntries(right_vector);
	for (int base_idx = 0; base_idx < count; base_idx++) {
		auto lhs_list_index = lhs_data.sel->get_index(base_idx);
		auto rhs_list_index = rhs_data.sel->get_index(base_idx);
		if (!lhs_data.validity.RowIsValid(lhs_list_index) || !rhs_data.validity.RowIsValid(rhs_list_index)) {
			FlatVector::SetNull(result, base_idx, true);
			continue;
		}
		for (size_t col = 0; col < child_entries.size(); ++col) {
			auto &child_entry = child_entries[col];
			auto &left_child_entry = left_child_entries[col];
			auto &right_child_entry = right_child_entries[col];
			auto pdata = ConstantVector::GetData<int32_t>(*child_entry);
			auto left_pdata = ConstantVector::GetData<int32_t>(*left_child_entry);
			auto right_pdata = ConstantVector::GetData<int32_t>(*right_child_entry);
			pdata[base_idx] = left_pdata[lhs_list_index] - right_pdata[rhs_list_index];
		}
	}
	if (left_vector_type == VectorType::CONSTANT_VECTOR && right_vector_type == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	result.Verify(count);
}

//===--------------------------------------------------------------------===//
// Quack Table Function
//===--------------------------------------------------------------------===//
class QuackFunction : public TableFunction {
public:
	QuackFunction() {
		name = "quack";
		arguments.push_back(LogicalType::BIGINT);
		bind = QuackBind;
		init_global = QuackInit;
		function = QuackFunc;
	}

	struct QuackBindData : public TableFunctionData {
		QuackBindData(idx_t number_of_quacks) : number_of_quacks(number_of_quacks) {
		}

		idx_t number_of_quacks;
	};

	struct QuackGlobalData : public GlobalTableFunctionState {
		QuackGlobalData() : offset(0) {
		}

		idx_t offset;
	};

	static duckdb::unique_ptr<FunctionData> QuackBind(ClientContext &context, TableFunctionBindInput &input,
	                                                  vector<LogicalType> &return_types, vector<string> &names) {
		names.emplace_back("quack");
		return_types.emplace_back(LogicalType::VARCHAR);
		return make_uniq<QuackBindData>(BigIntValue::Get(input.inputs[0]));
	}

	static duckdb::unique_ptr<GlobalTableFunctionState> QuackInit(ClientContext &context,
	                                                              TableFunctionInitInput &input) {
		return make_uniq<QuackGlobalData>();
	}

	static void QuackFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
		auto &bind_data = data_p.bind_data->Cast<QuackBindData>();
		auto &data = (QuackGlobalData &)*data_p.global_state;
		if (data.offset >= bind_data.number_of_quacks) {
			// finished returning values
			return;
		}
		// start returning values
		// either fill up the chunk or return all the remaining columns
		idx_t count = 0;
		while (data.offset < bind_data.number_of_quacks && count < STANDARD_VECTOR_SIZE) {
			output.SetValue(0, count, Value("QUACK"));
			data.offset++;
			count++;
		}
		output.SetCardinality(count);
	}
};

//===--------------------------------------------------------------------===//
// Parser extension
//===--------------------------------------------------------------------===//
struct QuackExtensionData : public ParserExtensionParseData {
	QuackExtensionData(idx_t number_of_quacks) : number_of_quacks(number_of_quacks) {
	}

	idx_t number_of_quacks;

	duckdb::unique_ptr<ParserExtensionParseData> Copy() const override {
		return make_uniq<QuackExtensionData>(number_of_quacks);
	}

	string ToString() const override {
		vector<string> quacks;
		for (idx_t i = 0; i < number_of_quacks; i++) {
			quacks.push_back("QUACK");
		}
		return StringUtil::Join(quacks, " ");
	}
};

class QuackExtension : public ParserExtension {
public:
	QuackExtension() {
		parse_function = QuackParseFunction;
		plan_function = QuackPlanFunction;
		parser_override = QuackParser;
	}

	static ParserExtensionParseResult QuackParseFunction(ParserExtensionInfo *info, const string &query) {
		auto lcase = StringUtil::Lower(query);
		if (!StringUtil::Contains(lcase, "quack")) {
			// quack not found!?
			if (StringUtil::Contains(lcase, "quac")) {
				// use our error
				return ParserExtensionParseResult("Did you mean... QUACK!?");
			}
			// use original error
			return ParserExtensionParseResult();
		}

		idx_t count = 0;
		size_t pos = 0;
		size_t last_end = 0;
		while ((pos = lcase.find("quack", last_end)) != string::npos) {
			string between = lcase.substr(last_end, pos - last_end);
			StringUtil::Trim(between);
			if (!between.empty() && !StringUtil::CIEquals(between, ";")) {
				return ParserExtensionParseResult("This is not a quack: " + between);
			}
			count++;
			last_end = pos + 5;
		}

		string after = lcase.substr(last_end);
		StringUtil::Trim(after);
		if (!after.empty() && !StringUtil::CIEquals(after, ";")) {
			return ParserExtensionParseResult("This is not a quack: " + after);
		}

		// QUACK
		return ParserExtensionParseResult(make_uniq<QuackExtensionData>(count));
	}

	static ParserExtensionPlanResult QuackPlanFunction(ParserExtensionInfo *info, ClientContext &context,
	                                                   duckdb::unique_ptr<ParserExtensionParseData> parse_data) {
		auto &quack_data = (QuackExtensionData &)*parse_data;

		ParserExtensionPlanResult result;
		result.function = QuackFunction();
		result.parameters.push_back(Value::BIGINT(quack_data.number_of_quacks));
		result.requires_valid_transaction = false;
		result.return_type = StatementReturnType::QUERY_RESULT;
		return result;
	}

	static ParserOverrideResult QuackParser(ParserExtensionInfo *info, const string &query, ParserOptions &options) {
		vector<string> queries = StringUtil::Split(query, ";");
		vector<unique_ptr<SQLStatement>> statements;
		for (const auto &query_input : queries) {
			if (StringUtil::CIEquals(query_input, "override")) {
				auto select_node = make_uniq<SelectNode>();
				select_node->select_list.push_back(
				    make_uniq<ConstantExpression>(Value("The DuckDB parser has been overridden")));
				select_node->from_table = make_uniq<EmptyTableRef>();
				auto select_statement = make_uniq<SelectStatement>();
				select_statement->node = std::move(select_node);
				statements.push_back(std::move(select_statement));
			}
			if (StringUtil::CIEquals(query_input, "overri")) {
				auto exception = ParserException("Parser overridden, query equaled \"overri\" but not \"override\"");
				return ParserOverrideResult(exception);
			}
		}
		if (statements.empty()) {
			auto not_implemented_exception =
			    NotImplementedException("QuackParser has not yet implemented the statements to transform this query");
			return ParserOverrideResult(not_implemented_exception);
		}
		return ParserOverrideResult(std::move(statements));
	}
};

//===--------------------------------------------------------------------===//
// Planner extension - adds an extra constant column to every query
//===--------------------------------------------------------------------===//
class AddColumnExtension : public PlannerExtension {
public:
	AddColumnExtension() {
		post_bind_function = AddColumnPostBind;
	}

	static void AddColumnPostBind(PlannerExtensionInput &input, BoundStatement &statement) {
		// Check if extension is enabled
		Value enabled;
		if (!input.context.TryGetCurrentSetting("add_column_enabled", enabled) || !enabled.GetValue<bool>()) {
			return;
		}

		// Only modify statements that return query results (SELECT, INSERT/UPDATE/DELETE RETURNING, etc.)
		auto &properties = input.binder.GetStatementProperties();
		if (properties.return_type != StatementReturnType::QUERY_RESULT) {
			return;
		}

		// Get the column bindings from the existing plan
		auto column_bindings = statement.plan->GetColumnBindings();

		// Get a new table index for the projection
		auto table_index = input.binder.GenerateTableIndex();

		// Create references to all existing columns using BoundColumnRefExpression
		vector<unique_ptr<Expression>> projections;
		for (idx_t i = 0; i < column_bindings.size(); i++) {
			projections.push_back(make_uniq<BoundColumnRefExpression>(statement.types[i], column_bindings[i]));
		}

		// Add a constant column
		projections.push_back(make_uniq<BoundConstantExpression>(Value("quack")));

		// Create a projection operator wrapping the existing plan
		auto projection = make_uniq<LogicalProjection>(table_index, std::move(projections));
		projection->children.push_back(std::move(statement.plan));
		projection->ResolveOperatorTypes();

		// Update the statement with the new plan, names, and types
		statement.plan = std::move(projection);
		statement.names.push_back("extra_column");
		statement.types.push_back(LogicalType::VARCHAR);
	}
};

static set<string> test_loaded_extension_list;

class QuackLoadExtension : public ExtensionCallback {
	void OnExtensionLoaded(DatabaseInstance &db, const string &name) override {
		test_loaded_extension_list.insert(name);
	}
};

static inline void LoadedExtensionsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	string result_str;
	for (auto &ext : test_loaded_extension_list) {
		if (!result_str.empty()) {
			result_str += ", ";
		}
		result_str += ext;
	}
	result.Reference(Value(result_str));
}
//===--------------------------------------------------------------------===//
// Bounded type
//===--------------------------------------------------------------------===//

struct BoundedType {
	static LogicalType Bind(BindLogicalTypeInput &input) {
		auto &modifiers = input.modifiers;

		if (modifiers.size() != 1) {
			throw BinderException("BOUNDED type must have one modifier");
		}
		if (modifiers[0].GetValue().type() != LogicalType::INTEGER) {
			throw BinderException("BOUNDED type modifier must be integer");
		}
		if (modifiers[0].GetValue().IsNull()) {
			throw BinderException("BOUNDED type modifier cannot be NULL");
		}
		auto bound_val = modifiers[0].GetValue().GetValue<int32_t>();
		return Get(bound_val);
	}

	static LogicalType Get(int32_t max_val) {
		auto type = LogicalType(LogicalTypeId::INTEGER);
		type.SetAlias("BOUNDED");
		auto info = make_uniq<ExtensionTypeInfo>();
		info->modifiers.emplace_back(Value::INTEGER(max_val));
		type.SetExtensionInfo(std::move(info));
		return type;
	}

	static LogicalType GetDefault() {
		auto type = LogicalType(LogicalTypeId::INTEGER);
		type.SetAlias("BOUNDED");
		return type;
	}

	static int32_t GetMaxValue(const LogicalType &type) {
		if (!type.HasExtensionInfo()) {
			throw InvalidInputException("BOUNDED type must have a max value");
		}
		auto &mods = type.GetExtensionInfo()->modifiers;
		if (mods[0].value.IsNull()) {
			throw InvalidInputException("BOUNDED type must have a max value");
		}
		return mods[0].value.GetValue<int32_t>();
	}
};

static void BoundedMaxFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	result.Reference(BoundedType::GetMaxValue(args.data[0].GetType()));
}

static unique_ptr<FunctionData> BoundedMaxBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	if (arguments[0]->return_type == BoundedType::GetDefault()) {
		bound_function.arguments[0] = arguments[0]->return_type;
	} else {
		throw BinderException("bounded_max expects a BOUNDED type");
	}
	return nullptr;
}

static void BoundedAddFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &left_vector = args.data[0];
	auto &right_vector = args.data[1];
	const auto count = args.size();
	BinaryExecutor::Execute<int32_t, int32_t, int32_t>(left_vector, right_vector, result, count,
	                                                   [&](int32_t left, int32_t right) { return left + right; });
}

static unique_ptr<FunctionData> BoundedAddBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	if (BoundedType::GetDefault() == arguments[0]->return_type &&
	    BoundedType::GetDefault() == arguments[1]->return_type) {
		auto left_max_val = BoundedType::GetMaxValue(arguments[0]->return_type);
		auto right_max_val = BoundedType::GetMaxValue(arguments[1]->return_type);

		auto new_max_val = left_max_val + right_max_val;
		bound_function.arguments[0] = arguments[0]->return_type;
		bound_function.arguments[1] = arguments[1]->return_type;
		bound_function.SetReturnType(BoundedType::Get(new_max_val));
	} else {
		throw BinderException("bounded_add expects two BOUNDED types");
	}
	return nullptr;
}

struct BoundedFunctionData : public FunctionData {
	int32_t max_val;

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<BoundedFunctionData>();
		copy->max_val = max_val;
		return std::move(copy);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<BoundedFunctionData>();
		return max_val == other.max_val;
	}
};

static unique_ptr<FunctionData> BoundedInvertBind(ClientContext &context, ScalarFunction &bound_function,
                                                  vector<unique_ptr<Expression>> &arguments) {
	if (arguments[0]->return_type == BoundedType::GetDefault()) {
		bound_function.arguments[0] = arguments[0]->return_type;
		bound_function.SetReturnType(arguments[0]->return_type);
	} else {
		throw BinderException("bounded_invert expects a BOUNDED type");
	}
	auto result = make_uniq<BoundedFunctionData>();
	result->max_val = BoundedType::GetMaxValue(bound_function.GetReturnType());
	return std::move(result);
}

static void BoundedInvertFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &source_vector = args.data[0];
	const auto count = args.size();

	auto result_type = result.GetType();
	auto output_max_val = BoundedType::GetMaxValue(result_type);

	UnaryExecutor::Execute<int32_t, int32_t>(source_vector, result, count,
	                                         [&](int32_t input) { return std::min(-input, output_max_val); });
}

static void BoundedEvenFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &source_vector = args.data[0];
	const auto count = args.size();
	UnaryExecutor::Execute<int32_t, bool>(source_vector, result, count, [&](int32_t input) { return input % 2 == 0; });
}

static void BoundedToAsciiFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &source_vector = args.data[0];
	const auto count = args.size();

	UnaryExecutor::Execute<int32_t, string_t>(source_vector, result, count, [&](int32_t input) {
		if (input < 0) {
			throw NotImplementedException("Negative values not supported");
		}
		string s;
		s.push_back(static_cast<char>(input));
		return StringVector::AddString(result, s);
	});
}

static bool BoundedToBoundedCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto input_max_val = BoundedType::GetMaxValue(source.GetType());
	auto output_max_val = BoundedType::GetMaxValue(result.GetType());

	if (input_max_val <= output_max_val) {
		result.Reinterpret(source);
		return true;
	} else {
		throw ConversionException(source.GetType(), result.GetType());
	}
}

static bool IntToBoundedCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &ty = result.GetType();
	auto output_max_val = BoundedType::GetMaxValue(ty);
	UnaryExecutor::Execute<int32_t, int32_t>(source, result, count, [&](int32_t input) {
		if (input > output_max_val) {
			throw ConversionException(StringUtil::Format("Value %s exceeds max value of bounded type (%s)",
			                                             to_string(input), to_string(output_max_val)));
		}
		return input;
	});
	return true;
}

//===--------------------------------------------------------------------===//
//  MINMAX type
//===--------------------------------------------------------------------===//
// This is like the BOUNDED type, except it has a custom bind_modifiers function
// to verify that the range is valid

struct MinMaxType {
	static LogicalType Bind(BindLogicalTypeInput &input) {
		auto &modifiers = input.modifiers;

		if (modifiers.size() != 2) {
			throw BinderException("MINMAX type must have two modifiers");
		}
		if (modifiers[0].GetValue().type() != LogicalType::INTEGER ||
		    modifiers[1].GetValue().type() != LogicalType::INTEGER) {
			throw BinderException("MINMAX type modifiers must be integers");
		}
		if (modifiers[0].GetValue().IsNull() || modifiers[1].GetValue().IsNull()) {
			throw BinderException("MINMAX type modifiers cannot be NULL");
		}

		const auto min_val = modifiers[0].GetValue().GetValue<int32_t>();
		const auto max_val = modifiers[1].GetValue().GetValue<int32_t>();

		if (min_val >= max_val) {
			throw BinderException("MINMAX type min value must be less than max value");
		}

		auto type = LogicalType(LogicalTypeId::INTEGER);
		type.SetAlias("MINMAX");
		auto info = make_uniq<ExtensionTypeInfo>();
		info->modifiers.emplace_back(Value::INTEGER(min_val));
		info->modifiers.emplace_back(Value::INTEGER(max_val));
		type.SetExtensionInfo(std::move(info));
		return type;
	}

	static int32_t GetMinValue(const LogicalType &type) {
		D_ASSERT(type.HasExtensionInfo());
		auto &mods = type.GetExtensionInfo()->modifiers;
		return mods[0].value.GetValue<int32_t>();
	}

	static int32_t GetMaxValue(const LogicalType &type) {
		D_ASSERT(type.HasExtensionInfo());
		auto &mods = type.GetExtensionInfo()->modifiers;
		return mods[1].value.GetValue<int32_t>();
	}

	static LogicalType Get(int32_t min_val, int32_t max_val) {
		auto type = LogicalType(LogicalTypeId::INTEGER);
		type.SetAlias("MINMAX");
		auto info = make_uniq<ExtensionTypeInfo>();
		info->modifiers.emplace_back(Value::INTEGER(min_val));
		info->modifiers.emplace_back(Value::INTEGER(max_val));
		type.SetExtensionInfo(std::move(info));
		return type;
	}

	static LogicalType GetDefault() {
		auto type = LogicalType(LogicalTypeId::INTEGER);
		type.SetAlias("MINMAX");
		return type;
	}
};

static bool IntToMinMaxCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &ty = result.GetType();
	auto min_val = MinMaxType::GetMinValue(ty);
	auto max_val = MinMaxType::GetMaxValue(ty);
	UnaryExecutor::Execute<int32_t, int32_t>(source, result, count, [&](int32_t input) {
		if (input < min_val || input > max_val) {
			throw ConversionException(StringUtil::Format("Value %s is outside of range [%s,%s]", to_string(input),
			                                             to_string(min_val), to_string(max_val)));
		}
		return input;
	});
	return true;
}

static void MinMaxRangeFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &ty = args.data[0].GetType();
	auto min_val = MinMaxType::GetMinValue(ty);
	auto max_val = MinMaxType::GetMaxValue(ty);
	result.Reference(Value::INTEGER(max_val - min_val));
}

//===--------------------------------------------------------------------===//
// Row ID Filter — extensible table filter demo
//
// This demo shows how an optimizer extension can inject an ExpressionFilter
// into a table scan's filter set. The filter wraps a ScalarFunction that
// checks each row's ROW_ID against a sorted allow-list.
//
//
// Three callbacks are involved:
//   1. RowIdFilterFunction  — execution: stateful linear scan over allow-list
//   2. RowIdFilterPropagate — row-group pruning via min/max statistics
//   3. RowIdFilterInit      — per-thread state initialization
//===--------------------------------------------------------------------===//

// The bind callback is unused for most extensible table filters
static unique_ptr<FunctionData> RowIdFilterBind(ClientContext &, ScalarFunction &, vector<unique_ptr<Expression>> &) {
	throw InternalException("rowid_filter: bind should never be called");
}

struct RowIdFilterBindData : public FunctionData {
	vector<int64_t> allowed_ids;
	unordered_set<int64_t> allowed_set;

	explicit RowIdFilterBindData(vector<int64_t> ids) : allowed_ids(std::move(ids)) {
		allowed_set.insert(allowed_ids.begin(), allowed_ids.end());
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<RowIdFilterBindData>(allowed_ids);
	}
	bool Equals(const FunctionData &other_p) const override {
		return allowed_ids == other_p.Cast<RowIdFilterBindData>().allowed_ids;
	}
};

// Per-thread local state for the filter function.
// In this demo we use a simple hash-set lookup, so no per-thread state is needed.
// In practice, if row IDs arrive in non-decreasing order (e.g. sequential scan),
// a cursor-based linear scan over a sorted allowed list would be more efficient — O(n) total.
struct RowIdFilterState : public FunctionLocalState {};

static unique_ptr<FunctionLocalState> RowIdFilterInit(ExpressionState &, const BoundFunctionExpression &,
                                                      FunctionData *) {
	return make_uniq<RowIdFilterState>();
}

static void RowIdFilterFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &bind_data = state.expr.Cast<BoundFunctionExpression>().bind_info->Cast<RowIdFilterBindData>();
	auto &allowed = bind_data.allowed_set;

	auto &input_vec = args.data[0];
	idx_t count = args.size();

	UnifiedVectorFormat vdata;
	input_vec.ToUnifiedFormat(count, vdata);
	auto row_ids = UnifiedVectorFormat::GetData<int64_t>(vdata);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto out = FlatVector::GetData<bool>(result);

	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		if (!vdata.validity.RowIsValid(idx)) {
			out[i] = false;
			continue;
		}
		out[i] = allowed.count(row_ids[idx]) > 0;
	}
}

static FilterPropagateResult RowIdFilterPropagate(const FunctionStatisticsPruneInput &input) {
	auto &allowed = input.bind_data->Cast<RowIdFilterBindData>().allowed_ids;
	auto &stats = input.stats;

	if (!NumericStats::HasMinMax(stats)) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	auto min_val = NumericStats::GetMin<int64_t>(stats);
	auto max_val = NumericStats::GetMax<int64_t>(stats);

	auto it = std::lower_bound(allowed.begin(), allowed.end(), min_val);
	if (it != allowed.end() && *it <= max_val) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	return FilterPropagateResult::FILTER_ALWAYS_FALSE;
}

class RowIdOptimizerExtension : public OptimizerExtension {
public:
	RowIdOptimizerExtension() {
		optimize_function = RowIdOptimizeFunction;
	}

	static void RowIdOptimizeFunction(OptimizerExtensionInput &input, duckdb::unique_ptr<LogicalOperator> &plan) {
		for (auto &child : plan->children) {
			RowIdOptimizeFunction(input, child);
		}
		if (plan->type != LogicalOperatorType::LOGICAL_GET) {
			return;
		}
		auto &get = plan->Cast<LogicalGet>();
		auto table = get.GetTable();
		if (!table || table->name != "rowid_test_table") {
			return;
		}

		// Build the scalar function
		ScalarFunction func("rowid_filter", {LogicalType::BIGINT}, LogicalType::BOOLEAN, RowIdFilterFunction,
		                    RowIdFilterBind);
		func.SetInitStateCallback(RowIdFilterInit);
		func.SetFilterPruneCallback(RowIdFilterPropagate);

		// Construct the bound expression (column index 0: the filter chunk contains only the filtered column)
		vector<unique_ptr<Expression>> children;
		children.push_back(make_uniq<BoundReferenceExpression>(LogicalType::BIGINT, 0));
		auto expr = make_uniq<BoundFunctionExpression>(LogicalType::BOOLEAN, func, std::move(children),
		                                               make_uniq<RowIdFilterBindData>(vector<int64_t> {3, 4, 5, 7, 9}));

		// Push the filter on the ROW_ID column
		get.table_filters.PushFilter(ColumnIndex(COLUMN_IDENTIFIER_ROW_ID),
		                             make_uniq<ExpressionFilter>(std::move(expr)));

		// Ensure ROW_ID is in the scan's column list
		bool has_rowid = false;
		for (auto &col : get.GetColumnIds()) {
			if (col.IsRowIdColumn()) {
				has_rowid = true;
				break;
			}
		}
		if (!has_rowid) {
			if (get.projection_ids.empty()) {
				for (idx_t i = 0; i < get.GetColumnIds().size(); i++) {
					get.projection_ids.push_back(i);
				}
			}
			get.AddColumnId(COLUMN_IDENTIFIER_ROW_ID);
		}
	}
};

//===--------------------------------------------------------------------===//
// Extension load + setup
//===--------------------------------------------------------------------===//
extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(loadable_extension_demo, loader) {
	CreateScalarFunctionInfo hello_alias_info(
	    ScalarFunction("test_alias_hello", {}, LogicalType::VARCHAR, TestAliasHello));

	auto &db = loader.GetDatabaseInstance();
	// create a scalar function
	Connection con(db);
	auto &client_context = *con.context;
	auto &catalog = Catalog::GetSystemCatalog(client_context);
	con.BeginTransaction();
	con.CreateScalarFunction<int32_t, string_t>("hello", {LogicalType(LogicalTypeId::VARCHAR)},
	                                            LogicalType(LogicalTypeId::INTEGER), &hello_fun);
	catalog.CreateFunction(client_context, hello_alias_info);

	// Add alias POINT type
	string alias_name = "POINT";
	child_list_t<LogicalType> child_types;
	child_types.push_back(make_pair("x", LogicalType::INTEGER));
	child_types.push_back(make_pair("y", LogicalType::INTEGER));
	auto alias_info = make_uniq<CreateTypeInfo>();
	alias_info->internal = true;
	alias_info->name = alias_name;
	LogicalType target_type = LogicalType::STRUCT(child_types);
	target_type.SetAlias(alias_name);
	alias_info->type = target_type;

	auto type_entry = catalog.CreateType(client_context, *alias_info);
	type_entry->tags["ext:name"] = "loadable_extension_demo";
	type_entry->tags["ext:author"] = "DuckDB Labs";

	// Function add point
	ScalarFunction add_point_func("add_point", {target_type, target_type}, target_type, AddPointFunction);
	CreateScalarFunctionInfo add_point_info(add_point_func);
	auto add_point_entry = catalog.CreateFunction(client_context, add_point_info);
	add_point_entry->tags["ext:name"] = "loadable_extension_demo";
	add_point_entry->tags["ext:author"] = "DuckDB Labs";

	// Function sub point
	ScalarFunction sub_point_func("sub_point", {target_type, target_type}, target_type, SubPointFunction);
	CreateScalarFunctionInfo sub_point_info(sub_point_func);
	auto sub_point_entry = catalog.CreateFunction(client_context, sub_point_info);
	sub_point_entry->tags["ext:name"] = "loadable_extension_demo";
	sub_point_entry->tags["ext:author"] = "DuckDB Labs";

	// Function sub point
	ScalarFunction loaded_extensions("loaded_extensions", {}, LogicalType::VARCHAR, LoadedExtensionsFunction);
	CreateScalarFunctionInfo loaded_extensions_info(loaded_extensions);
	catalog.CreateFunction(client_context, loaded_extensions_info);

	// Quack function
	QuackFunction quack_function;
	CreateTableFunctionInfo quack_info(quack_function);
	catalog.CreateTableFunction(client_context, quack_info);

	con.Commit();

	// add a parser extension
	auto &config = DBConfig::GetConfig(db);
	ParserExtension::Register(config, QuackExtension());
	ExtensionCallback::Register(config, make_shared_ptr<QuackLoadExtension>());

	// add a planner extension that adds an extra column to queries
	PlannerExtension::Register(config, AddColumnExtension());
	config.AddExtensionOption("add_column_enabled", "enable adding extra column to queries", LogicalType::BOOLEAN,
	                          Value::BOOLEAN(false));

	// Bounded type
	auto bounded_type = BoundedType::GetDefault();
	loader.RegisterType("BOUNDED", bounded_type, BoundedType::Bind);

	// Example of function inspecting the type property
	ScalarFunction bounded_max("bounded_max", {bounded_type}, LogicalType::INTEGER, BoundedMaxFunc, BoundedMaxBind);
	loader.RegisterFunction(bounded_max);

	// Example of function inspecting the type property and returning the same type
	ScalarFunction bounded_invert("bounded_invert", {bounded_type}, bounded_type, BoundedInvertFunc, BoundedInvertBind);
	// bounded_invert.serialize = BoundedReturnSerialize;
	// bounded_invert.deserialize = BoundedReturnDeserialize;
	loader.RegisterFunction(bounded_invert);

	// Example of function inspecting the type property of both arguments and returning a new type
	ScalarFunction bounded_add("bounded_add", {bounded_type, bounded_type}, bounded_type, BoundedAddFunc,
	                           BoundedAddBind);
	loader.RegisterFunction(bounded_add);

	// Example of function that is generic over the type property (the bound is not important)
	ScalarFunction bounded_even("bounded_even", {bounded_type}, LogicalType::BOOLEAN, BoundedEvenFunc);
	loader.RegisterFunction(bounded_even);

	// Example of function that is specialized over type property
	auto bounded_specialized_type = BoundedType::Get(0xFF);
	ScalarFunction bounded_to_ascii("bounded_ascii", {bounded_specialized_type}, LogicalType::VARCHAR,
	                                BoundedToAsciiFunc);
	loader.RegisterFunction(bounded_to_ascii);

	// Enable explicit casting to our specialized type
	loader.RegisterCastFunction(bounded_type, bounded_specialized_type, BoundCastInfo(BoundedToBoundedCast), 0);
	// Casts
	loader.RegisterCastFunction(LogicalType::INTEGER, bounded_type, BoundCastInfo(IntToBoundedCast), 0);

	// MinMax Type
	auto minmax_type = MinMaxType::GetDefault();
	loader.RegisterType("MINMAX", minmax_type, MinMaxType::Bind);
	loader.RegisterCastFunction(LogicalType::INTEGER, minmax_type, BoundCastInfo(IntToMinMaxCast), 0);
	loader.RegisterFunction(ScalarFunction("minmax_range", {minmax_type}, LogicalType::INTEGER, MinMaxRangeFunc));

	// Register the RowId optimizer extension (extensible table filter demo)
	config.GetCallbackManager().Register(RowIdOptimizerExtension());
}
}
