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

//! Struct that holds the configuration of a CSV State Machine
//! Basically which char, quote and escape were used to generate it.
struct CSVStateMachineConfig {
	CSVStateMachineConfig(char delimiter_p, char quote_p, char escape_p): delimiter(delimiter_p), quote(quote_p), escape(escape_p){};
	char delimiter;
	char quote;
	char escape;

	bool operator==(const CSVStateMachineConfig &other) const {
	    return delimiter == other.delimiter && quote == other.quote && escape == other.escape;
	}
};

//! Hash function used in out state machine cache, it hashes and combines all options used to generate a state machine
struct HashCSVStateMachineConfig {
	size_t operator()(CSVStateMachineConfig const& config) const noexcept {
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
	state_machine_t &Get(char delimiter, char quote, char escape);

private:
	void Insert(char delimiter, char quote, char escape);
	//! Cache on delimiter|quote|escape
	unordered_map<CSVStateMachineConfig, state_machine_t,HashCSVStateMachineConfig> state_machine_cache;
};
} // namespace duckdb
