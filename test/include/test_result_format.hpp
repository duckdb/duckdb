//===----------------------------------------------------------------------===//
//                         DuckDB
//
// test_result_format.hpp
//
// A deterministic result format for the format tests: it concatenates chunks into units of a fixed
// row count and records, per unit, the producer that built it.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/main/buffered_data/buffered_data.hpp"
#include "duckdb/main/result_format.hpp"
#include "duckdb/main/result_unit.hpp"

#ifndef DUCKDB_NO_THREADS

#include <thread>

namespace duckdb {

class TestUnit : public ResultUnit {
public:
	TestUnit(vector<unique_ptr<DataChunk>> chunks_p, std::thread::id producer_p, idx_t rows, idx_t bytes)
	    : ResultUnit(rows, bytes), chunks(std::move(chunks_p)), producer(producer_p) {
	}

public:
	unique_ptr<ResultUnit> Copy() const override {
		vector<unique_ptr<DataChunk>> copies;
		for (auto &chunk : chunks) {
			copies.push_back(BufferedData::CopyForBuffering(*chunk));
		}
		return make_uniq<TestUnit>(std::move(copies), producer, row_count, byte_size);
	}

public:
	vector<unique_ptr<DataChunk>> chunks;
	std::thread::id producer;
};

class TestFormatGlobalState : public ResultFormatGlobalState {
public:
	TestFormatGlobalState(vector<LogicalType> types_p, vector<Identifier> names_p, ResultOrdering ordering_p)
	    : types(std::move(types_p)), names(std::move(names_p)), ordering(ordering_p) {
	}

public:
	vector<LogicalType> types;
	vector<Identifier> names;
	ResultOrdering ordering;
	//! How many worker threads built a unit for this query
	atomic<idx_t> local_states {0};
	//! Units finished short of the row cap: one per batch boundary and one per producer at its end
	atomic<idx_t> partial_units {0};
};

class TestFormatLocalState : public ResultFormatLocalState {
public:
	vector<unique_ptr<DataChunk>> chunks;
	std::thread::id producer;
	idx_t rows = 0;
	idx_t bytes = 0;
};

class TestFormat : public ResultFormat {
public:
	using Unit = TestUnit;
	using GlobalState = TestFormatGlobalState;
	static constexpr const char *NAME = "test";

public:
	explicit TestFormat(idx_t max_unit_rows_p) : max_unit_rows(max_unit_rows_p) {
	}

public:
	const char *Name() const override {
		return NAME;
	}

	unique_ptr<ResultFormatGlobalState> InitGlobal(const vector<LogicalType> &types, const vector<Identifier> &names,
	                                               const ClientProperties &properties,
	                                               ResultOrdering ordering) override {
		return make_uniq<TestFormatGlobalState>(types, names, ordering);
	}

	unique_ptr<ResultFormatLocalState> InitLocal(ResultFormatGlobalState &gstate) override {
		gstate.Cast<TestFormatGlobalState>().local_states++;
		return make_uniq<TestFormatLocalState>();
	}

	void Append(ResultFormatGlobalState &gstate, ResultFormatLocalState &lstate_p, DataChunk &chunk) override {
		if (throw_in_append) {
			throw InvalidInputException("TestFormat::Append");
		}
		auto &lstate = lstate_p.Cast<TestFormatLocalState>();
		if (lstate.chunks.empty()) {
			lstate.producer = std::this_thread::get_id();
		}
		auto copy = BufferedData::CopyForBuffering(chunk);
		lstate.rows += copy->size();
		lstate.bytes += copy->GetDataSize();
		lstate.chunks.push_back(std::move(copy));
	}

	unique_ptr<ResultUnit> Finish(ResultFormatGlobalState &gstate, ResultFormatLocalState &lstate_p,
	                              bool flush_partial) override {
		auto &lstate = lstate_p.Cast<TestFormatLocalState>();
		if (lstate.chunks.empty() || (lstate.rows < max_unit_rows && !flush_partial)) {
			return nullptr;
		}
		if (throw_in_finish) {
			throw InvalidInputException("TestFormat::Finish");
		}
		auto unit = make_uniq<TestUnit>(std::move(lstate.chunks), lstate.producer, lstate.rows, lstate.bytes);
		if (lstate.rows < max_unit_rows) {
			gstate.Cast<TestFormatGlobalState>().partial_units++;
		}
		lstate.chunks.clear();
		lstate.rows = 0;
		lstate.bytes = 0;
		return std::move(unit);
	}

public:
	idx_t max_unit_rows;
	atomic<bool> throw_in_append {false};
	atomic<bool> throw_in_finish {false};
};

//! Declares the same unit type as TestFormat, so only the name check can refuse the mismatch
class OtherTestFormat : public ResultFormat {
public:
	using Unit = TestUnit;
	using GlobalState = TestFormatGlobalState;
	static constexpr const char *NAME = "other";

public:
	const char *Name() const override {
		return NAME;
	}

	unique_ptr<ResultFormatGlobalState> InitGlobal(const vector<LogicalType> &types, const vector<Identifier> &names,
	                                               const ClientProperties &properties,
	                                               ResultOrdering ordering) override {
		return make_uniq<TestFormatGlobalState>(types, names, ordering);
	}

	unique_ptr<ResultFormatLocalState> InitLocal(ResultFormatGlobalState &gstate) override {
		return make_uniq<TestFormatLocalState>();
	}

	void Append(ResultFormatGlobalState &gstate, ResultFormatLocalState &lstate, DataChunk &chunk) override {
	}

	unique_ptr<ResultUnit> Finish(ResultFormatGlobalState &gstate, ResultFormatLocalState &lstate,
	                              bool flush_partial) override {
		return nullptr;
	}
};

inline vector<Value> UnitValues(const ResultUnit &unit, idx_t column) {
	vector<Value> values;
	for (auto &chunk : unit.Cast<TestUnit>().chunks) {
		for (idx_t row = 0; row < chunk->size(); row++) {
			values.push_back(chunk->GetValue(column, row));
		}
	}
	return values;
}

inline unique_ptr<QueryResult> SubmitFormatted(Connection &con, const string &query, idx_t max_unit_rows) {
	auto handle = con.Submit(query);
	REQUIRE(!handle->HasError());
	handle->SetFormat(make_shared_ptr<TestFormat>(max_unit_rows));
	return handle;
}

inline void RequireAscending(const vector<int64_t> &rows, idx_t expected_count) {
	REQUIRE(rows.size() == expected_count);
	for (idx_t i = 0; i < rows.size(); i++) {
		if (rows[i] != NumericCast<int64_t>(i)) {
			FAIL(StringUtil::Format("Out-of-order row %llu: expected %llu, got %lld", i, i, rows[i]));
		}
	}
}

} // namespace duckdb

#endif
