#include "duckdb/function/table/range.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

struct CheckpointBindData : public FunctionData {
	explicit CheckpointBindData(optional_ptr<AttachedDatabase> db) : db(db) {
	}

	optional_ptr<AttachedDatabase> db;

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<CheckpointBindData>(db);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<CheckpointBindData>();
		return db == other.db;
	}
};

static unique_ptr<FunctionData> CheckpointBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");

	optional_ptr<AttachedDatabase> db;
	auto &db_manager = DatabaseManager::Get(context);
	if (!input.inputs.empty()) {
		if (input.inputs[0].IsNull()) {
			throw BinderException("Database cannot be NULL");
		}
		auto &db_name = StringValue::Get(input.inputs[0]);
		db = db_manager.GetDatabase(context, db_name);
		if (!db) {
			throw BinderException("Database \"%s\" not found", db_name);
		}
	} else {
		db = db_manager.GetDatabase(context, DatabaseManager::GetDefaultDatabase(context));
	}
	return make_uniq<CheckpointBindData>(db);
}

template <bool FORCE>
static void TemplatedCheckpointFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<CheckpointBindData>();
	auto &transaction_manager = TransactionManager::Get(*bind_data.db.get_mutable());
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
