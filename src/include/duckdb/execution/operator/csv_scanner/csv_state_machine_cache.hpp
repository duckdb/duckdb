//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/csv_scanner/csv_state_machine_cache.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/object_cache.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/execution/operator/csv_scanner/state_machine_options.hpp"
#include "duckdb/execution/operator/csv_scanner/quote_rules.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_state.hpp"

namespace duckdb {

//! Class to wrap the state machine matrix
class StateMachine {
public:
	static constexpr uint32_t NUM_STATES = 15;
	static constexpr uint32_t NUM_TRANSITIONS = 256;
	CSVState state_machine[NUM_TRANSITIONS][NUM_STATES];
	//! Transitions where we might skip processing
	//! For the Standard State
	bool skip_standard[256];
	//! For the Quoted State
	bool skip_quoted[256];
	//! For the Comment State
	bool skip_comment[256];

	uint64_t delimiter = 0;
	uint64_t new_line = 0;
	uint64_t carriage_return = 0;
	uint64_t quote = 0;
	uint64_t escape = 0;
	uint64_t comment = 0;

	const CSVState *operator[](const idx_t i) const {
		return state_machine[i];
	}

	CSVState *operator[](const idx_t i) {
		return state_machine[i];
	}
};

//! Hash function used in out state machine cache, it hashes and combines all options used to generate a state machine
struct HashCSVStateMachineConfig {
	size_t operator()(CSVStateMachineOptions const &config) const noexcept {
		auto h_delimiter = Hash(config.delimiter.GetValue());
		auto h_quote = Hash(config.quote.GetValue());
		auto h_escape = Hash(config.escape.GetValue());
		auto h_newline = Hash(static_cast<uint8_t>(config.new_line.GetValue()));
		auto h_comment = Hash(static_cast<uint8_t>(config.comment.GetValue()));
		return CombineHash(h_delimiter, CombineHash(h_quote, CombineHash(h_escape, CombineHash(h_newline, h_comment))));
	}
};

//! The CSVStateMachineCache caches state machines, although small ~2kb, the actual creation of multiple State Machines
//! can become a bottleneck on sniffing, when reading very small csv files.
//! Hence, the cache stores State Machines based on their different delimiter|quote|escape options.
class CSVStateMachineCache : public ObjectCacheEntry {
public:
	CSVStateMachineCache();
	~CSVStateMachineCache() override = default;
	//! Gets a state machine from the cache, if it's not from one the default options
	//! It first caches it, then returns it.
	static CSVStateMachineCache &Get(ClientContext &context);

	//! Gets a state machine from the cache, if it's not from one the default options
	//! It first caches it, then returns it.
	const StateMachine &Get(const CSVStateMachineOptions &state_machine_options);

	static string ObjectType() {
		return "CSV_STATE_MACHINE_CACHE";
	}

	string GetObjectType() override {
		return ObjectType();
	}

private:
	void Insert(const CSVStateMachineOptions &state_machine_options);
	//! Cache on delimiter|quote|escape|newline
	unordered_map<CSVStateMachineOptions, StateMachine, HashCSVStateMachineConfig> state_machine_cache;
	//! Default value for options used to initialize CSV State Machine Cache

	//! Because the state machine cache can be accessed in Parallel we need a mutex.
	mutex main_mutex;
};
} // namespace duckdb
