#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "re2/re2.h"

#include <iostream>

namespace duckdb {

static unordered_map<column_t, string> GetKnownColumnValues(string &filename,
                                                            unordered_map<string, column_t> &column_map,
                                                            bool filename_col, bool hive_partition_cols) {
	unordered_map<column_t, string> result;

	if (filename_col) {
		auto lookup_column_id = column_map.find("filename");
		if (lookup_column_id != column_map.end()) {
			result[lookup_column_id->second] = filename;
		}
	}

	if (hive_partition_cols) {
		auto partitions = HivePartitioning::Parse(filename);
		for (auto &partition : partitions) {
			auto lookup_column_id = column_map.find(partition.first);
			if (lookup_column_id != column_map.end()) {
				result[lookup_column_id->second] = partition.second;
			}
		}
	}

	return result;
}

// Takes an expression and converts a list of known column_refs to constants
static void ConvertKnownColRefToConstants(unique_ptr<Expression> &expr,
                                          unordered_map<column_t, string> &known_column_values, idx_t table_index) {
	if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
		auto &bound_colref = (BoundColumnRefExpression &)*expr;

		// This bound column ref is for another table
		if (table_index != bound_colref.binding.table_index) {
			return;
		}

		auto lookup = known_column_values.find(bound_colref.binding.column_index);
		if (lookup != known_column_values.end()) {
			expr = make_unique<BoundConstantExpression>(Value(lookup->second));
		}
	} else {
		ExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<Expression> &child) {
			ConvertKnownColRefToConstants(child, known_column_values, table_index);
		});
	}
}

// matches hive partitions in file name. For example:
// 	- s3://bucket/var1=value1/bla/bla/var2=value2
//  - http(s)://domain(:port)/lala/kasdl/var1=value1/?not-a-var=not-a-value
//  - folder/folder/folder/../var1=value1/etc/.//var2=value2
std::map<string, string> HivePartitioning::Parse(string &filename) {
	std::map<string, string> result;

	string regex = "[\\/\\\\]([^\\/\\?\\\\]+)=([^\\/\\n\\?\\\\]+)";
	duckdb_re2::StringPiece input(filename); // Wrap a StringPiece around it

	string var;
	string value;
	while (RE2::FindAndConsume(&input, regex, &var, &value)) {
		result.insert(std::pair<string, string>(var, value));
	}
	return result;
}

// TODO: this can still be improved by removing the parts of filter expressions that are true for all remaining files.
//		 currently, only expressions that cannot be evaluated during pushdown are removed.
void HivePartitioning::ApplyFiltersToFileList(vector<string> &files, vector<unique_ptr<Expression>> &filters,
                                              unordered_map<string, column_t> &column_map, idx_t table_index,
                                              bool hive_enabled, bool filename_enabled) {
	vector<string> pruned_files;
	vector<unique_ptr<Expression>> pruned_filters;

	if ((!filename_enabled && !hive_enabled) || filters.empty()) {
		return;
	}

	for (idx_t i = 0; i < files.size(); i++) {
		auto &file = files[i];
		bool should_prune_file = false;
		auto known_values = GetKnownColumnValues(file, column_map, filename_enabled, hive_enabled);

		FilterCombiner combiner;
		for (auto &filter : filters) {
			unique_ptr<Expression> filter_copy = filter->Copy();
			ConvertKnownColRefToConstants(filter_copy, known_values, table_index);
			// Evaluate the filter, if it can be evaluated here, we can not prune this filter
			Value result_value;
			if (!filter_copy->IsScalar() || !filter_copy->IsFoldable() ||
			    !ExpressionExecutor::TryEvaluateScalar(*filter_copy, result_value)) {
				// can not be evaluated only with the filename/hive columns added, we can not prune this filter
				pruned_filters.emplace_back(filter->Copy());
			} else if (!result_value.GetValue<bool>()) {
				// filter evaluates to false
				should_prune_file = true;
			}

			// Use filter combiner to determine that this filter makes
			if (!should_prune_file && combiner.AddFilter(std::move(filter_copy)) == FilterResult::UNSATISFIABLE) {
				should_prune_file = true;
			}
		}

		if (!should_prune_file) {
			pruned_files.push_back(file);
		}
	}

	filters = std::move(pruned_filters);
	files = std::move(pruned_files);
}

} // namespace duckdb
