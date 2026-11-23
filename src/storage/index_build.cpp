#include "duckdb/storage/table/index_build.hpp"

#include "duckdb/common/algorithm.hpp"

namespace duckdb {

IndexBuild::IndexBuild(transaction_t transaction_id_p, transaction_t build_commit_p)
    : transaction_id(transaction_id_p), build_commit(build_commit_p) {
}

void IndexBuild::AddChanges(const IndexBuildRange &rows, idx_t changed_rows) {
	changes.ranges.push_back(rows);
	changes.changed_rows += changed_rows;
}

IndexBuildChanges IndexBuild::TakeChanges() {
	IndexBuildChanges result;
	std::swap(result, changes);
	MergeRanges(result.ranges);
	return result;
}

void IndexBuild::MergeRanges(vector<IndexBuildRange> &ranges) {
	std::sort(ranges.begin(), ranges.end(),
	          [](const IndexBuildRange &a, const IndexBuildRange &b) { return a.start_row < b.start_row; });
	idx_t merged = 0;
	for (auto &range : ranges) {
		if (merged > 0 && range.start_row <= ranges[merged - 1].End()) {
			auto &last = ranges[merged - 1];
			last.count = MaxValue(last.End(), range.End()) - last.start_row;
			continue;
		}
		ranges[merged++] = range;
	}
	ranges.resize(merged);
}

} // namespace duckdb
