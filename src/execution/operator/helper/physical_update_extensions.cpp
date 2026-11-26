#include "duckdb/execution/operator/helper/physical_update_extensions.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/parallel/callback_async_task.hpp"

namespace duckdb {

//! Runs the pending updates as async tasks: BLOCKED while they run, HAVE_MORE_OUTPUT when they ran synchronously
static SourceResultType ScheduleUpdates(ExecutionContext &context, UpdateExtensionsGlobalState &data,
                                        OperatorSourceInput &input) {
	auto &client = context.client;
	vector<unique_ptr<AsyncTask>> tasks;
	for (auto &pending : data.pending_updates) {
		auto &result = data.update_result_entries[pending.result_index];
		tasks.push_back(make_uniq<CallbackAsyncTask>(
		    [&client, &pending, &result]() { ExtensionHelper::ExecuteExtensionUpdate(client, pending, result); }));
	}
	return AsyncResult::FromTasks(std::move(tasks), TaskSchedulerType::ASYNC).ScheduleOrExecute(context, input, data);
}

SourceResultType PhysicalUpdateExtensions::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                           OperatorSourceInput &input) const {
	auto &data = input.global_state.Cast<UpdateExtensionsGlobalState>();

	if (!data.updates_scheduled) {
		data.updates_scheduled = true;
		if (!data.pending_updates.empty()) {
			return ScheduleUpdates(context, data, input);
		}
	}

	if (data.offset >= data.update_result_entries.size()) {
		// finished returning values
		return SourceResultType::FINISHED;
	}

	idx_t count = 0;

	// extension_name VARCHAR
	auto &extension_name = chunk.data[0];
	// repository VARCHAR
	auto &repository = chunk.data[1];
	// update_result VARCHAR
	auto &update_result = chunk.data[2];
	// previous_version VARCHAR
	auto &previous_version = chunk.data[3];
	// current_version VARCHAR
	auto &current_version = chunk.data[4];
	// error VARCHAR
	auto &error = chunk.data[5];

	while (data.offset < data.update_result_entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.update_result_entries[data.offset];

		extension_name.Append(Value(entry.extension_name));
		repository.Append(Value(entry.repository));
		update_result.Append(Value(EnumUtil::ToString(entry.tag)));
		previous_version.Append(Value(entry.prev_version));
		current_version.Append(Value(entry.installed_version));
		if (entry.tag == ExtensionUpdateResultTag::UPDATE_FAILED) {
			error.Append(Value(entry.error));
		} else {
			error.Append(Value(LogicalType::VARCHAR));
		}

		data.offset++;
		count++;
	}

	return data.offset >= data.update_result_entries.size() ? SourceResultType::FINISHED
	                                                        : SourceResultType::HAVE_MORE_OUTPUT;
}

unique_ptr<GlobalSourceState> PhysicalUpdateExtensions::GetGlobalSourceState(ClientContext &context) const {
	auto res = make_uniq<UpdateExtensionsGlobalState>();

	if (info->extensions_to_update.empty()) {
		// Update all
		ExtensionHelper::PrepareExtensionUpdates(context, res->update_result_entries, res->pending_updates);
	} else {
		// Update extensions in extensions_to_update
		for (const auto &ext : info->extensions_to_update) {
			ExtensionHelper::PrepareExtensionUpdate(context, ext.GetIdentifierName(), res->update_result_entries,
			                                        res->pending_updates);
		}
	}

	return std::move(res);
}

} // namespace duckdb
