#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "re2/re2.h"

#include <iostream>

namespace duckdb {

static std::map<string, string> GetKnownColumnValues(string &filename, bool filename_col, bool hive_partition_cols) {
	std::map<string, string> result;

	if (filename_col) {
		result["filename"] = filename;
	}

	if (hive_partition_cols) {
		auto partitions = HivePartitioning::Parse(filename);
		for (auto &partition : partitions) {
			result[partition.first] = partition.second;
		}
	}

	return result;
}

// Takes an expression and converts a list of known column_refs to constants
static void ConvertKnownColRefToConstants(unique_ptr<Expression> &expr, map<string, string> &known_column_values) {
	if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
		auto lookup = known_column_values.find(expr->alias);
		if (lookup != known_column_values.end()) {
			expr = make_unique<BoundConstantExpression>(Value(lookup->second));
		}
	} else {
		ExpressionIterator::EnumerateChildren(
		    *expr, [&](unique_ptr<Expression> &child) { ConvertKnownColRefToConstants(child, known_column_values); });
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

void HivePartitioning::PruneFilesList(vector<string> &files, vector<unique_ptr<Expression>> &filters, bool hive_enabled,
                                      bool filename_enabled, bool preserve_first) {
	vector<string> pruned_files;

	if (preserve_first) {
		pruned_files.push_back(files[0]);
	}

	for (idx_t i = preserve_first; i < files.size(); i++) {
		auto &file = files[i];
		bool should_prune_file = false;
		auto known_values = GetKnownColumnValues(file, filename_enabled, hive_enabled);

		FilterCombiner combiner;
		for (auto &filter : filters) {
			unique_ptr<Expression> filter_copy = filter->Copy();
			ConvertKnownColRefToConstants(filter_copy, known_values);
			if (combiner.AddFilter(std::move(filter_copy)) == FilterResult::UNSATISFIABLE) {
				should_prune_file = true;
				break;
			}
		}

		if (!should_prune_file) {
			pruned_files.push_back(file);
		}
	}

	files = std::move(pruned_files);
}

} // namespace duckdb
