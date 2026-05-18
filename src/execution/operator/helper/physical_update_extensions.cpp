#include "duckdb/execution/operator/helper/physical_update_extensions.hpp"
#include "duckdb/main/extension_helper.hpp"

namespace duckdb {

SourceResultType PhysicalUpdateExtensions::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                           OperatorSourceInput &input) const {
	auto &data = input.global_state.Cast<UpdateExtensionsGlobalState>();

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

	while (data.offset < data.update_result_entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.update_result_entries[data.offset];

		extension_name.Append(Value(entry.extension_name));
		repository.Append(Value(entry.repository));
		update_result.Append(Value(EnumUtil::ToString(entry.tag)));
		previous_version.Append(Value(entry.prev_version));
		current_version.Append(Value(entry.installed_version));

		data.offset++;
		count++;
	}
	chunk.SetCardinality(count);

	return data.offset >= data.update_result_entries.size() ? SourceResultType::FINISHED
	                                                        : SourceResultType::HAVE_MORE_OUTPUT;
}

unique_ptr<GlobalSourceState> PhysicalUpdateExtensions::GetGlobalSourceState(ClientContext &context) const {
	auto res = make_uniq<UpdateExtensionsGlobalState>();

	if (info->extensions_to_update.empty()) {
		// Update all
		res->update_result_entries = ExtensionHelper::UpdateExtensions(context);
	} else {
		// Update extensions in extensions_to_update
		for (const auto &ext : info->extensions_to_update) {
			res->update_result_entries.emplace_back(ExtensionHelper::UpdateExtension(context, ext));
		}
	}

	return std::move(res);
}

} // namespace duckdb
