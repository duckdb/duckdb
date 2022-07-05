#define DUCKDB_EXTENSION_MAIN
#include "duckdb.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/common/string_util.hpp"
using namespace duckdb;

//===--------------------------------------------------------------------===//
// Scalar function
//===--------------------------------------------------------------------===//
inline string_t hello_fun(string_t what) {
	return "Hello, " + what.GetString();
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

	static unique_ptr<FunctionData> QuackBind(ClientContext &context, TableFunctionBindInput &input,
	                                          vector<LogicalType> &return_types, vector<string> &names) {
		names.emplace_back("quack");
		return_types.emplace_back(LogicalType::VARCHAR);
		return make_unique<QuackBindData>(BigIntValue::Get(input.inputs[0]));
	}

	static unique_ptr<GlobalTableFunctionState> QuackInit(ClientContext &context, TableFunctionInitInput &input) {
		return make_unique<QuackGlobalData>();
	}

	static void QuackFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
		auto &bind_data = (QuackBindData &)*data_p.bind_data;
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

	unique_ptr<ParserExtensionParseData> Copy() const override {
		return make_unique<QuackExtensionData>(number_of_quacks);
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
		return ParserExtensionParseResult(make_unique<QuackExtensionData>(splits.size() + 1));
	}

	static ParserExtensionPlanResult QuackPlanFunction(ParserExtensionInfo *info, ClientContext &context,
	                                                   unique_ptr<ParserExtensionParseData> parse_data) {
		auto &quack_data = (QuackExtensionData &)*parse_data;

		ParserExtensionPlanResult result;
		result.function = QuackFunction();
		result.parameters.push_back(Value::BIGINT(quack_data.number_of_quacks));
		result.read_only = true;
		result.requires_valid_transaction = false;
		result.return_type = StatementReturnType::QUERY_RESULT;
		return result;
	}
};

//===--------------------------------------------------------------------===//
// Extension load + setup
//===--------------------------------------------------------------------===//
extern "C" {
DUCKDB_EXTENSION_API void loadable_extension_demo_init(duckdb::DatabaseInstance &db) {
	// create a scalar function
	Connection con(db);
	con.BeginTransaction();
	con.CreateScalarFunction<string_t, string_t>("hello", {LogicalType(LogicalTypeId::VARCHAR)},
	                                             LogicalType(LogicalTypeId::VARCHAR), &hello_fun);
	con.Commit();

	// add a parser extension
	auto &config = DBConfig::GetConfig(db);
	config.parser_extensions.push_back(QuackExtension());
}

DUCKDB_EXTENSION_API const char *loadable_extension_demo_version() {
	return DuckDB::LibraryVersion();
}
}
