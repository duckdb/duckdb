//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/csv_scanner/csv_state_machine_cache.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/persistent/csv_scanner/csv_reader_options.hpp"
#include "duckdb/execution/operator/persistent/csv_scanner/csv_buffer_manager.hpp"

namespace duckdb {
static constexpr uint32_t NUM_STATES = 7;
static constexpr uint32_t NUM_TRANSITIONS = 256;
typedef uint8_t state_machine_t[NUM_STATES][NUM_TRANSITIONS];

//! The CSVStateMachineCache caches state machines, although small ~2kb, the actual creation of multiple State Machines
//! can become a bottleneck on sniffing, when reading very small csv files.
//! Hence the cache stores State Machines based on their different delimiter|quote|escape options.
class CSVStateMachineCache {
public:
	CSVStateMachineCache();
	~CSVStateMachineCache() {};
	//! Gets a state machine from the cache, if it's not from one the default options
	//! It first caches it, then returns it.
	state_machine_t &Get(char delimiter, char quote, char escape);

private:
	void Insert(char delimiter, char quote, char escape);
	//! Cache on delimiter|quote|escape
	unordered_map<char, unordered_map<char, unordered_map<char, state_machine_t>>> state_machine_cache;
};
} // namespace duckdb
