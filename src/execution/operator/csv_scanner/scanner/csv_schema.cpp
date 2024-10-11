#include "duckdb/execution/operator/csv_scanner/csv_schema.hpp"

namespace duckdb {

struct TypeIdxPair {
	TypeIdxPair(LogicalType type_p, idx_t idx_p) : type(std::move(type_p)), idx(idx_p) {
	}
	TypeIdxPair() {
	}
	LogicalType type;
	idx_t idx {};
};

// We only really care about types that can be set in the sniffer_auto, or are sniffed by default
// If the user manually sets them, we should never get a cast issue from the sniffer!
bool CSVSchema::CanWeCastIt(LogicalTypeId source, LogicalTypeId destination) {
	if (destination == LogicalTypeId::VARCHAR || source == destination) {
		// We can always cast to varchar
		// And obviously don't have to do anything if they are equal.
		return true;
	}
	switch (source) {
	case LogicalTypeId::SQLNULL:
		return true;
	case LogicalTypeId::TINYINT:
		return destination == LogicalTypeId::SMALLINT || destination == LogicalTypeId::INTEGER ||
		       destination == LogicalTypeId::BIGINT || destination == LogicalTypeId::DECIMAL ||
		       destination == LogicalTypeId::FLOAT || destination == LogicalTypeId::DOUBLE;
	case LogicalTypeId::SMALLINT:
		return destination == LogicalTypeId::INTEGER || destination == LogicalTypeId::BIGINT ||
		       destination == LogicalTypeId::DECIMAL || destination == LogicalTypeId::FLOAT ||
		       destination == LogicalTypeId::DOUBLE;
	case LogicalTypeId::INTEGER:
		return destination == LogicalTypeId::BIGINT || destination == LogicalTypeId::DECIMAL ||
		       destination == LogicalTypeId::FLOAT || destination == LogicalTypeId::DOUBLE;
	case LogicalTypeId::BIGINT:
		return destination == LogicalTypeId::DECIMAL || destination == LogicalTypeId::FLOAT ||
		       destination == LogicalTypeId::DOUBLE;
	case LogicalTypeId::FLOAT:
		return destination == LogicalTypeId::DOUBLE;
	default:
		return false;
	}
}

void CSVSchema::Initialize(vector<string> &names, vector<LogicalType> &types, const string &file_path_p) {
	if (!columns.empty()) {
		throw InternalException("CSV Schema is already populated, this should not happen.");
	}
	file_path = file_path_p;
	D_ASSERT(names.size() == types.size() && !names.empty());
	for (idx_t i = 0; i < names.size(); i++) {
		// Populate our little schema
		columns.push_back({names[i], types[i]});
		name_idx_map[names[i]] = i;
	}
}

bool CSVSchema::Empty() const {
	return columns.empty();
}

bool CSVSchema::SchemasMatch(string &error_message, vector<string> &names, vector<LogicalType> &types,
                             const string &cur_file_path) {
	D_ASSERT(names.size() == types.size());
	bool match = true;
	unordered_map<string, TypeIdxPair> current_schema;
	for (idx_t i = 0; i < names.size(); i++) {
		// Populate our little schema
		current_schema[names[i]] = {types[i], i};
	}
	// Here we check if the schema of a given file matched our original schema
	// We consider it's not a match if:
	// 1. The file misses columns that were defined in the original schema.
	// 2. They have a column match, but the types do not match.
	std::ostringstream error;
	error << "Schema mismatch between globbed files."
	      << "\n";
	error << "Main file schema: " << file_path << "\n";
	error << "Current file: " << cur_file_path << "\n";

	for (auto &column : columns) {
		if (current_schema.find(column.name) == current_schema.end()) {
			error << "Column with name: \"" << column.name << "\" is missing"
			      << "\n";
			match = false;
		} else {
			if (!CanWeCastIt(current_schema[column.name].type.id(), column.type.id())) {
				error << "Column with name: \"" << column.name
				      << "\" is expected to have type: " << column.type.ToString();
				error << " But has type: " << current_schema[column.name].type.ToString() << "\n";
				match = false;
			}
		}
	}

	// Lets suggest some potential fixes
	error << "Potential Fix: Since your schema has a mismatch, consider setting union_by_name=true.";
	if (!match) {
		error_message = error.str();
	}
	return match;
}

} // namespace duckdb
