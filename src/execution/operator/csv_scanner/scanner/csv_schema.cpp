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

void CSVSchema::MergeSchemas(CSVSchema &other, bool null_padding) {
	// TODO: We could also merge names, maybe by giving preference to non-generated names?
	const vector<LogicalType> candidates_by_specificity = {LogicalType::BOOLEAN, LogicalType::BIGINT,
	                                                       LogicalType::DOUBLE, LogicalType::VARCHAR};
	for (idx_t i = 0; i < columns.size() && i < other.columns.size(); i++) {
		auto this_type = columns[i].type.id();
		auto other_type = other.columns[i].type.id();
		if (columns[i].type != other.columns[i].type) {
			if (CanWeCastIt(this_type, other_type)) {
				// If we can cast this to other, this becomes other
				columns[i].type = other.columns[i].type;
			} else if (!CanWeCastIt(other_type, this_type)) {
				// If we can't cast this to other or other to this, we see which parent they can be both cast to
				for (const auto &type : candidates_by_specificity) {
					if (CanWeCastIt(this_type, type.id()) && CanWeCastIt(other_type, type.id())) {
						columns[i].type = type;
						break;
					}
				}
			}
		}
	}

	if (null_padding && other.columns.size() > columns.size()) {
		for (idx_t i = columns.size(); i < other.columns.size(); i++) {
			auto name = other.columns[i].name;
			auto type = other.columns[i].type;
			columns.push_back({name, type});
			name_idx_map[name] = i;
		}
	}
}

CSVSchema::CSVSchema(const vector<string> &names, const vector<LogicalType> &types, const string &file_path,
                     idx_t rows_read_p, const bool empty_p)
    : rows_read(rows_read_p), empty(empty_p) {
	Initialize(names, types, file_path);
}

void CSVSchema::Initialize(const vector<string> &names, const vector<LogicalType> &types, const string &file_path_p) {
	if (!columns.empty()) {
		throw InternalException("CSV Schema is already populated, this should not happen.");
	}
	file_path = file_path_p;
	D_ASSERT(names.size() == types.size() && !names.empty());
	for (idx_t i = 0; i < names.size(); i++) {
		// Populate our little schema
		const auto &name = names.at(i);
		const auto &type = types.at(i);
		columns.push_back({name, type});
		name_idx_map[names[i]] = i;
	}
}

vector<string> CSVSchema::GetNames() const {
	vector<string> names;
	for (auto &column : columns) {
		names.push_back(column.name);
	}
	return names;
}

vector<LogicalType> CSVSchema::GetTypes() const {
	vector<LogicalType> types;
	for (auto &column : columns) {
		types.push_back(column.type);
	}
	return types;
}

bool CSVSchema::Empty() const {
	return columns.empty();
}

bool CSVSchema::MatchColumns(const CSVSchema &other) const {
	return other.columns.size() == columns.size() || empty || other.empty;
}

string CSVSchema::GetPath() const {
	return file_path;
}

idx_t CSVSchema::GetColumnCount() const {
	return columns.size();
}

idx_t CSVSchema::GetRowsRead() const {
	return rows_read;
}

bool CSVSchema::SchemasMatch(string &error_message, SnifferResult &sniffer_result, const string &cur_file_path,
                             bool is_minimal_sniffer) const {
	D_ASSERT(sniffer_result.names.size() == sniffer_result.return_types.size());
	bool match = true;
	unordered_map<string, TypeIdxPair> current_schema;

	for (idx_t i = 0; i < sniffer_result.names.size(); i++) {
		// Populate our little schema
		current_schema[sniffer_result.names[i]] = {sniffer_result.return_types[i], i};
	}
	if (is_minimal_sniffer) {
		auto min_sniffer = static_cast<AdaptiveSnifferResult &>(sniffer_result);
		if (!min_sniffer.more_than_one_row) {
			bool min_sniff_match = true;
			// If we don't have more than one row, either the names must match or the types must match.
			for (auto &column : columns) {
				if (current_schema.find(column.name) == current_schema.end()) {
					min_sniff_match = false;
					break;
				}
			}
			if (min_sniff_match) {
				return true;
			}
			// Otherwise, the types must match.
			min_sniff_match = true;
			if (sniffer_result.return_types.size() == columns.size()) {
				idx_t return_type_idx = 0;
				for (auto &column : columns) {
					if (column.type != sniffer_result.return_types[return_type_idx++]) {
						min_sniff_match = false;
						break;
					}
				}
			} else {
				min_sniff_match = false;
			}
			if (min_sniff_match) {
				// If we got here, we have the right types but the wrong names, lets fix the names
				idx_t sniff_name_idx = 0;
				for (auto &column : columns) {
					sniffer_result.names[sniff_name_idx++] = column.name;
				}
				return true;
			}
		}
		// If we got to this point, the minimal sniffer doesn't match, we throw an error.
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
