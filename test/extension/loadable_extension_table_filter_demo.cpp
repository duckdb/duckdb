#include "duckdb.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/table_filter_extension.hpp"
#include "duckdb/planner/filter/extension_table_filter.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/column_index.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

using namespace duckdb;

struct RowIdFilterState : public ExtensionFilterState {
	duckdb::vector<int64_t> allowed_ids;

	// Index of the next potential match in allowed_ids.
	// Since row IDs are scanned in increasing order, we can use this to optimize the filter.
	idx_t next_allowed_id = 0;

	// Pre-allocated SelectionVector to avoid repeated allocations during filtering.
	SelectionVector filter_sel;
	idx_t current_capacity = 0;
};

// RowIdFilter is a custom TableFilter that filters rows based on their Row ID.
// It only permits rows whose Row ID is present in the provided 'allowed_ids' list.
// The 'allowed_ids' list must be sorted in increasing order for optimizations to work.
class RowIdFilter : public ExtensionTableFilter {
public:
	RowIdFilter(duckdb::vector<int64_t> allowed_ids) : allowed_ids(std::move(allowed_ids)) {
	}

	duckdb::vector<int64_t> allowed_ids;

	// ToString returns a string representation of the filter, used for EXPLAIN output.
	string ToString(const string &column_name) const override {
		string result = "RowIDFilter (";
		for (size_t i = 0; i < allowed_ids.size(); i++) {
			if (i > 0) {
				result += ", ";
			}
			result += std::to_string(allowed_ids[i]);
		}
		result += ")";
		return result;
	}

	duckdb::unique_ptr<TableFilter> Copy() const override {
		return make_uniq<RowIdFilter>(allowed_ids);
	}

	void Serialize(Serializer &serializer) const override {
		TableFilter::Serialize(serializer);
		serializer.WriteProperty(200, "extension_name", string("row_id_filter"));
		serializer.WriteProperty(201, "allowed_ids", allowed_ids);
	}

	static duckdb::unique_ptr<TableFilter> Deserialize(Deserializer &deserializer) {
		auto allowed_ids = deserializer.ReadProperty<duckdb::vector<int64_t>>(201, "allowed_ids");
		return make_uniq<RowIdFilter>(std::move(allowed_ids));
	}

	unique_ptr<TableFilterState> InitializeState(ClientContext &context) const override {
		auto result = make_uniq<RowIdFilterState>();
		result->allowed_ids = allowed_ids;
		return std::move(result);
	}

	// CheckStatistics uses min/max statistics of a data block to prune it early.
	// It performs a binary search (std::lower_bound) on 'allowed_ids' to see if any
	// allowed ID falls within the [min, max] range of the block.
	FilterPropagateResult CheckStatistics(BaseStatistics &stats) const override {
		if (!NumericStats::HasMinMax(stats)) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		}
		auto min = NumericStats::GetMin<int64_t>(stats);
		auto max = NumericStats::GetMax<int64_t>(stats);

		// Efficiently check if any allowed_id is in [min, max]
		auto it = std::lower_bound(allowed_ids.begin(), allowed_ids.end(), min);
		if (it != allowed_ids.end() && *it <= max) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		}
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}

	// Filter applies the filter to a vector of row IDs.
	// It uses a two-pointer approach for efficiency:
	// 1. It assumes the input row IDs in 'keys_v' are increasing (standard for row IDs).
	// 2. It tracks the current position in 'allowed_ids' using 'state.next_allowed_id'.
	// This allows filtering the entire vector in a single linear pass.
	idx_t Filter(Vector &keys_v, SelectionVector &sel, idx_t &approved_tuple_count,
	             TableFilterState &state_p) const override {
		auto &state = state_p.Cast<RowIdFilterState>();
		auto &allowed = state.allowed_ids;

		if (state.current_capacity < approved_tuple_count) {
			state.filter_sel.Initialize(approved_tuple_count);
			state.current_capacity = approved_tuple_count;
		}

		UnifiedVectorFormat vdata;
		keys_v.ToUnifiedFormat(approved_tuple_count, vdata);
		auto data = UnifiedVectorFormat::GetData<int64_t>(vdata);

		idx_t found_count = 0;
		for (idx_t i = 0; i < approved_tuple_count; i++) {
			auto idx = sel.get_index(i);
			auto row_id = data[vdata.sel->get_index(idx)];

			// Advance next_allowed_id until it's >= current row_id
			while (state.next_allowed_id < allowed.size() && allowed[state.next_allowed_id] < row_id) {
				state.next_allowed_id++;
			}

			if (state.next_allowed_id < allowed.size() && allowed[state.next_allowed_id] == row_id) {
				state.filter_sel.set_index(found_count++, i);
			}
		}

		if (found_count == approved_tuple_count) {
			return approved_tuple_count;
		}

		if (sel.IsSet()) {
			for (idx_t i = 0; i < found_count; i++) {
				const idx_t flat_idx = state.filter_sel.get_index(i);
				const idx_t original_idx = sel.get_index(flat_idx);
				sel.set_index(i, original_idx);
			}
		} else {
			sel.Initialize(state.filter_sel);
		}

		approved_tuple_count = found_count;
		return found_count;
	}
};

// RowIdOptimizerExtension injects the RowIdFilter into the logical plan.
// It specifically targets LogicalGet operators on the table "rowid_test_table".
class RowIdOptimizerExtension : public OptimizerExtension {
public:
	RowIdOptimizerExtension() {
		optimize_function = RowIdOptimizeFunction;
	}

	static void RowIdOptimizeFunction(OptimizerExtensionInput &input, duckdb::unique_ptr<LogicalOperator> &plan) {
		if (plan->type == LogicalOperatorType::LOGICAL_GET) {
			auto &get = plan->Cast<LogicalGet>();
			// Check if we are querying the specific table "rowid_test_table"
			// We check if the table alias or name matches
			auto table = get.GetTable();
			if (table && table->name == "rowid_test_table") {
				// Inject filter: keep 3, 4, 5, 7, 9
				duckdb::vector<int64_t> allowed = {3, 4, 5, 7, 9};
				auto filter = make_uniq<RowIdFilter>(std::move(allowed));
				get.table_filters.PushFilter(ColumnIndex(COLUMN_IDENTIFIER_ROW_ID), std::move(filter));

				// Add ROW ID to column_ids if not present
				bool found = false;
				for (auto &col : get.GetColumnIds()) {
					if (col.IsRowIdColumn()) {
						found = true;
						break;
					}
				}
				if (!found) {
					// If projection_ids is empty, populate it with all current columns
					if (get.projection_ids.empty()) {
						for (idx_t i = 0; i < get.GetColumnIds().size(); i++) {
							get.projection_ids.push_back(i);
						}
					}
					get.AddColumnId(COLUMN_IDENTIFIER_ROW_ID);
				}
			}
		}

		for (auto &child : plan->children) {
			RowIdOptimizeFunction(input, child);
		}
	}
};

class RowIdTableFilterExtension : public TableFilterExtension {
public:
	string GetName() override {
		return "row_id_filter";
	}

	duckdb::unique_ptr<TableFilter> Deserialize(Deserializer &deserializer) override {
		return RowIdFilter::Deserialize(deserializer);
	}
};

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(loadable_extension_table_filter_demo, loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);
	config.table_filter_extensions.push_back(make_uniq<RowIdTableFilterExtension>());
	config.optimizer_extensions.push_back(RowIdOptimizerExtension());
}
}
