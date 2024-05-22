#include "duckdb/execution/operator/helper/physical_update_extensions.hpp"
#include "duckdb/main/extension_helper.hpp"

namespace duckdb {

SourceResultType PhysicalUpdateExtensions::GetData(ExecutionContext &context, DataChunk &chunk,
                                                   OperatorSourceInput &input) const {
	auto &data = input.global_state.Cast<UpdateExtensionsGlobalState>();

	if (data.offset >= data.update_result_entries.size()) {
		// finished returning values
		return SourceResultType::FINISHED;
	}

	idx_t count = 0;
	while (data.offset < data.update_result_entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.update_result_entries[data.offset];

		// return values:
		idx_t col = 0;
		// extension_name LogicalType::VARCHAR
		chunk.SetValue(col++, count, Value(entry.extension_name));
		// repository LogicalType::VARCHAR
		chunk.SetValue(col++, count, Value(entry.repository));
		// update_result
		chunk.SetValue(col++, count, Value(EnumUtil::ToString(entry.tag)));
		// previous_version LogicalType::VARCHAR
		chunk.SetValue(col++, count, Value(entry.prev_version));
		// current_version LogicalType::VARCHAR
		chunk.SetValue(col++, count, Value(entry.installed_version));

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
