#define DUCKDB_EXTENSION_MAIN
#include "duckdb.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/planner/extension_callback.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

using namespace duckdb;

//===--------------------------------------------------------------------===//
// Scalar function
//===--------------------------------------------------------------------===//
inline int32_t hello_fun(string_t what) {
	return what.GetSize() + 5;
}

inline void TestAliasHello(DataChunk &args, ExpressionState &state, Vector &result) {
	result.Reference(Value("Hello Alias!"));
}

inline void AddPointFunction(DataChunk &args, ExpressionState &state, Vector &result) {
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

inline void SubPointFunction(DataChunk &args, ExpressionState &state, Vector &result) {
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
	}

	static ParserExtensionParseResult QuackParseFunction(ParserExtensionInfo *info, const string &query) {
		auto lcase = StringUtil::Lower(StringUtil::Replace(query, ";", ""));
		if (!StringUtil::Contains(lcase, "quack")) {
			// quack not found!?
			if (StringUtil::Contains(lcase, "quac")) {
				// use our error
				return ParserExtensionParseResult("Did you mean... QUACK!?");
			}
			// use original error
			return ParserExtensionParseResult();
		}
		auto splits = StringUtil::Split(lcase, "quack");
		for (auto &split : splits) {
			StringUtil::Trim(split);
			if (!split.empty()) {
				// we only accept quacks here
				return ParserExtensionParseResult("This is not a quack: " + split);
			}
		}
		// QUACK
		return ParserExtensionParseResult(make_uniq<QuackExtensionData>(splits.size() + 1));
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
};

static set<string> test_loaded_extension_list;

class QuackLoadExtension : public ExtensionCallback {
	void OnExtensionLoaded(DatabaseInstance &db, const string &name) override {
		test_loaded_extension_list.insert(name);
	}
};

inline void LoadedExtensionsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
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
	static LogicalType Get(int32_t max_val) {
		auto type = LogicalType(LogicalTypeId::INTEGER);
		type.SetAlias("BOUNDED");
		type.SetModifiers({Value::INTEGER(max_val)});
		return type;
	}

	static LogicalType GetDefault() {
		auto type = LogicalType(LogicalTypeId::INTEGER);
		type.SetAlias("BOUNDED");
		// By default we set a NULL max value to indicate that it can be any value
		type.SetModifiers({Value(LogicalType::INTEGER)});
		return type;
	}

	static int32_t GetMaxValue(const LogicalType &type) {
		auto mods_ptr = type.GetModifiers();
		if (!mods_ptr) {
			throw InvalidInputException("BOUNDED type must have a max value");
		}
		auto &mods = *mods_ptr;
		if (mods[0].IsNull()) {
			throw InvalidInputException("BOUNDED type must have a max value");
		}
		return mods[0].GetValue<int32_t>();
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
		bound_function.return_type = BoundedType::Get(new_max_val);
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
		bound_function.return_type = arguments[0]->return_type;
	} else {
		throw BinderException("bounded_invert expects a BOUNDED type");
	}
	auto result = make_uniq<BoundedFunctionData>();
	result->max_val = BoundedType::GetMaxValue(bound_function.return_type);
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
	static LogicalType Bind(BindTypeModifiersInput &input) {
		auto &modifiers = input.modifiers;

		if (modifiers.size() != 2) {
			throw BinderException("MINMAX type must have two modifiers");
		}
		if (modifiers[0].type() != LogicalType::INTEGER || modifiers[1].type() != LogicalType::INTEGER) {
			throw BinderException("MINMAX type modifiers must be integers");
		}
		if (modifiers[0].IsNull() || modifiers[1].IsNull()) {
			throw BinderException("MINMAX type modifiers cannot be NULL");
		}

		auto min_val = modifiers[0].GetValue<int32_t>();
		auto max_val = modifiers[1].GetValue<int32_t>();
		if (min_val >= max_val) {
			throw BinderException("MINMAX type min value must be less than max value");
		}

		auto type = LogicalType(LogicalTypeId::INTEGER);
		type.SetAlias("MINMAX");
		type.SetModifiers({Value::INTEGER(min_val), Value::INTEGER(max_val)});
		return type;
	}

	static int32_t GetMinValue(const LogicalType &type) {
		D_ASSERT(type.HasModifiers());
		auto &mods = *type.GetModifiers();
		return mods[0].GetValue<int32_t>();
	}

	static int32_t GetMaxValue(const LogicalType &type) {
		D_ASSERT(type.HasModifiers());
		auto &mods = *type.GetModifiers();
		return mods[1].GetValue<int32_t>();
	}

	static LogicalType Get(int32_t min_val, int32_t max_val) {
		auto type = LogicalType(LogicalTypeId::INTEGER);
		type.SetAlias("MINMAX");
		type.SetModifiers({Value::INTEGER(min_val), Value::INTEGER(max_val)});
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
// Extension load + setup
//===--------------------------------------------------------------------===//
extern "C" {
DUCKDB_EXTENSION_API void loadable_extension_demo_init(duckdb::DatabaseInstance &db) {
	CreateScalarFunctionInfo hello_alias_info(
	    ScalarFunction("test_alias_hello", {}, LogicalType::VARCHAR, TestAliasHello));

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
	config.parser_extensions.push_back(QuackExtension());
	config.extension_callbacks.push_back(make_uniq<QuackLoadExtension>());

	// Bounded type
	auto bounded_type = BoundedType::GetDefault();
	ExtensionUtil::RegisterType(db, "BOUNDED", bounded_type);

	// Example of function inspecting the type property
	ScalarFunction bounded_max("bounded_max", {bounded_type}, LogicalType::INTEGER, BoundedMaxFunc, BoundedMaxBind);
	ExtensionUtil::RegisterFunction(db, bounded_max);

	// Example of function inspecting the type property and returning the same type
	ScalarFunction bounded_invert("bounded_invert", {bounded_type}, bounded_type, BoundedInvertFunc, BoundedInvertBind);
	// bounded_invert.serialize = BoundedReturnSerialize;
	// bounded_invert.deserialize = BoundedReturnDeserialize;
	ExtensionUtil::RegisterFunction(db, bounded_invert);

	// Example of function inspecting the type property of both arguments and returning a new type
	ScalarFunction bounded_add("bounded_add", {bounded_type, bounded_type}, bounded_type, BoundedAddFunc,
	                           BoundedAddBind);
	ExtensionUtil::RegisterFunction(db, bounded_add);

	// Example of function that is generic over the type property (the bound is not important)
	ScalarFunction bounded_even("bounded_even", {bounded_type}, LogicalType::BOOLEAN, BoundedEvenFunc);
	ExtensionUtil::RegisterFunction(db, bounded_even);

	// Example of function that is specialized over type property
	auto bounded_specialized_type = BoundedType::Get(0xFF);
	ScalarFunction bounded_to_ascii("bounded_ascii", {bounded_specialized_type}, LogicalType::VARCHAR,
	                                BoundedToAsciiFunc);
	ExtensionUtil::RegisterFunction(db, bounded_to_ascii);

	// Enable explicit casting to our specialized type
	ExtensionUtil::RegisterCastFunction(db, bounded_type, bounded_specialized_type, BoundCastInfo(BoundedToBoundedCast),
	                                    0);
	// Casts
	ExtensionUtil::RegisterCastFunction(db, LogicalType::INTEGER, bounded_type, BoundCastInfo(IntToBoundedCast), 0);

	// MinMax Type
	auto minmax_type = MinMaxType::GetDefault();
	ExtensionUtil::RegisterType(db, "MINMAX", minmax_type, MinMaxType::Bind);
	ExtensionUtil::RegisterCastFunction(db, LogicalType::INTEGER, minmax_type, BoundCastInfo(IntToMinMaxCast), 0);
	ExtensionUtil::RegisterFunction(
	    db, ScalarFunction("minmax_range", {minmax_type}, LogicalType::INTEGER, MinMaxRangeFunc));
}

DUCKDB_EXTENSION_API const char *loadable_extension_demo_version() {
	return DuckDB::LibraryVersion();
}
}
