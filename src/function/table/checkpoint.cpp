#include "duckdb/function/table/range.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

struct CheckpointBindData : public FunctionData {
	explicit CheckpointBindData(Identifier database_name) : database_name(std::move(database_name)) {
	}

	Identifier database_name;

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<CheckpointBindData>(database_name);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<CheckpointBindData>();
		return database_name == other.database_name;
	}
};

static unique_ptr<FunctionData> CheckpointBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<Identifier> &names) {
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");

	auto &db_manager = DatabaseManager::Get(context);
	Identifier database_name;
	if (!input.inputs.empty()) {
		if (input.inputs[0].IsNull()) {
			throw BinderException("Database cannot be NULL");
		}
		database_name = Identifier(StringValue::Get(input.inputs[0]));
	} else {
		database_name = DatabaseManager::GetDefaultDatabase(context);
	}
	if (!db_manager.GetDatabase(context, database_name)) {
		throw BinderException("Database %s not found", database_name);
	}
	return make_uniq<CheckpointBindData>(std::move(database_name));
}

template <bool FORCE>
static void TemplatedCheckpointFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<CheckpointBindData>();
	auto database = DatabaseManager::Get(context).GetDatabase(context, bind_data.database_name);
	if (!database) {
		throw BinderException("Database %s not found", bind_data.database_name);
	}
	auto &transaction_manager = TransactionManager::Get(*database);
	transaction_manager.Checkpoint(context, FORCE);
}

void CheckpointFunction::RegisterFunction(BuiltinFunctions &set) {
	TableFunctionSet checkpoint("checkpoint");
	checkpoint.AddFunction(TableFunction({}, TemplatedCheckpointFunction<false>, CheckpointBind));
	checkpoint.AddFunction(TableFunction({LogicalType::VARCHAR}, TemplatedCheckpointFunction<false>, CheckpointBind));
	set.AddFunction(checkpoint);

	TableFunctionSet force_checkpoint("force_checkpoint");
	force_checkpoint.AddFunction(TableFunction({}, TemplatedCheckpointFunction<true>, CheckpointBind));
	force_checkpoint.AddFunction(
	    TableFunction({LogicalType::VARCHAR}, TemplatedCheckpointFunction<true>, CheckpointBind));
	set.AddFunction(force_checkpoint);
}

} // namespace duckdb
