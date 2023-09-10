//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/csv/csv_state_machine_cache.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/scan/csv/csv_reader_options.hpp"
#include "duckdb/execution/operator/scan/csv/csv_buffer_manager.hpp"
#include "duckdb/execution/operator/scan/csv/quote_rules.hpp"

namespace duckdb {
static constexpr uint32_t NUM_STATES = 8;
static constexpr uint32_t NUM_TRANSITIONS = 256;
typedef uint8_t state_machine_t[NUM_STATES][NUM_TRANSITIONS];

//! Hash function used in out state machine cache, it hashes and combines all options used to generate a state machine
struct HashCSVStateMachineConfig {
	size_t operator()(CSVStateMachineOptions const &config) const noexcept {
		auto h_delimiter = Hash(config.delimiter);
		auto h_quote = Hash(config.quote);
		auto h_escape = Hash(config.escape);
		return CombineHash(h_delimiter, CombineHash(h_quote, h_escape));
	}
};

//! The CSVStateMachineCache caches state machines, although small ~2kb, the actual creation of multiple State Machines
//! can become a bottleneck on sniffing, when reading very small csv files.
//! Hence the cache stores State Machines based on their different delimiter|quote|escape options.
class CSVStateMachineCache {
public:
	CSVStateMachineCache();
	~CSVStateMachineCache() {};
	//! Gets a state machine from the cache, if it's not from one the default options
	//! It first caches it, then returns it.
	const state_machine_t &Get(const CSVStateMachineOptions &state_machine_options);

private:
	void Insert(const CSVStateMachineOptions &state_machine_options);
	//! Cache on delimiter|quote|escape
	unordered_map<CSVStateMachineOptions, state_machine_t, HashCSVStateMachineConfig> state_machine_cache;
	//! Default value for options used to intialize CSV State Machine Cache
	const vector<char> default_delimiter = {',', '|', ';', '\t'};
	const vector<vector<char>> default_quote = {{'\"'}, {'\"', '\''}, {'\0'}};
	const vector<QuoteRule> default_quote_rule = {QuoteRule::QUOTES_RFC, QuoteRule::QUOTES_OTHER, QuoteRule::NO_QUOTES};
	const vector<vector<char>> default_escape = {{'\0', '\"', '\''}, {'\\'}, {'\0'}};
};
} // namespace duckdb
