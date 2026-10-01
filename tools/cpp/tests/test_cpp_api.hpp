#pragma once

#include "catch.hpp"
#include "duckdb_cpp.hpp"

#include <string>
#include <utility>
#include <vector>

// Shared helpers for the Stable C++ API test files. Only helpers with more
// than one consumer file live here; single-file helpers stay in that file's
// anonymous namespace.

// Collect two columns of a result into rows, reading each column as its C
// type. Callers pass non-NULL columns; every row is asserted valid.
template <class TA, class TB>
std::vector<std::pair<TA, TB>> Collect2(duckdb::cxx::QueryResult result, idx_t a, idx_t b) {
	std::vector<std::pair<TA, TB>> rows;
	while (auto chunk = result.FetchChunk()) {
		auto va = chunk.GetVector(a).GetView();
		auto vb = chunk.GetVector(b).GetView();
		auto pa = va.Data<TA>();
		auto pb = vb.Data<TB>();
		for (idx_t i = 0; i < chunk.GetRowCount(); i++) {
			REQUIRE(va.IsValid(i));
			REQUIRE(vb.IsValid(i));
			rows.emplace_back(pa[va.SelAt(i)], pb[vb.SelAt(i)]);
		}
	}
	return rows;
}
