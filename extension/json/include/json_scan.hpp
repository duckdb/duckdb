//===----------------------------------------------------------------------===//
//                         DuckDB
//
// json_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "json_reader.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/types/type_map.hpp"
#include "duckdb/function/scalar/strftime_format.hpp"
#include "duckdb/function/table_function.hpp"
#include "json_enums.hpp"
#include "json_transform.hpp"
#include "json_reader_options.hpp"

namespace duckdb {

//! Where a column of a GeoJSON Feature scan reads its value from. A Feature has two independent namespaces - its
//! own members and its "properties" - which can both contain the same name, so columns are resolved by position
//! rather than by name. The key is kept separately from the column name, which may have been deduplicated.
struct JSONFeatureColumn {
	//! Whether to read from the Feature's "properties" object instead of from the Feature itself
	bool from_properties;
	//! The JSON key to read
	string key;
};

struct JSONScanData : public TableFunctionData {
public:
	JSONScanData();

	void InitializeFormats();
	void InitializeFormats(bool auto_detect);

public:
	//! JSON reader options
	JSONReaderOptions options;

	//! The set of keys to extract (case sensitive)
	vector<string> key_names;
	//! For JSONRecordType::FEATURES: where each column reads its value from, in bind order
	vector<JSONFeatureColumn> feature_columns;

	//! The date format map
	unique_ptr<DateFormatMap> date_format_map;
	//! Options when transforming the JSON to columnar data
	JSONTransformOptions transform_options;

	optional_idx max_threads;
	optional_idx estimated_cardinality_per_file;
};

struct JSONScanInfo : public TableFunctionInfo {
public:
	explicit JSONScanInfo(JSONScanType type_p = JSONScanType::INVALID, JSONFormat format_p = JSONFormat::AUTO_DETECT,
	                      JSONRecordType record_type_p = JSONRecordType::AUTO_DETECT, bool auto_detect_p = false)
	    : type(type_p), format(format_p), record_type(record_type_p), auto_detect(auto_detect_p) {
	}

	JSONScanType type;
	JSONFormat format;
	JSONRecordType record_type;
	bool auto_detect;
};

struct JSONScanGlobalState {
public:
	JSONScanGlobalState(ClientContext &context, const JSONScanData &json_data, idx_t total_file_count);

public:
	//! Bound data
	const JSONScanData &json_data;
	//! Options when transforming the JSON to columnar data
	JSONTransformOptions transform_options;

	//! Column names that we're actually reading (after projection pushdown)
	vector<string> names;
	vector<column_t> column_ids;
	vector<ColumnIndex> column_indices;
	//! For JSONRecordType::FEATURES: parallel to names, where each of them reads its value from
	vector<JSONFeatureColumn> feature_columns;

	//! Buffer manager allocator
	Allocator &allocator;
	//! The current buffer capacity
	idx_t buffer_capacity;

	//! Current number of threads active
	idx_t system_threads;
	//! Whether we enable parallel scans (only if less files than threads)
	bool enable_parallel_scans;

	bool file_is_assigned = false;
	bool initialized = false;
};

struct JSONScanLocalState {
public:
	JSONScanLocalState(ClientContext &context, JSONScanGlobalState &gstate);

public:
	idx_t Read();
	void AddTransformError(idx_t object_index, const string &error_message);

	JSONReaderScanState &GetScanState() {
		return scan_state;
	}

	const JSONReaderScanState &GetScanState() const {
		return scan_state;
	}

	bool TryInitializeScan(JSONScanGlobalState &gstate, JSONReader &reader);

public:
	//! Options when transforming the JSON to columnar data
	JSONTransformOptions transform_options;

private:
	void ParseJSON(char *const json_start, const idx_t json_size, const idx_t remaining);

private:
	//! Scan state
	JSONReaderScanState scan_state;
};

struct JSONGlobalTableFunctionState : public GlobalTableFunctionState {
public:
	JSONGlobalTableFunctionState(ClientContext &context, const JSONScanData &json_data, idx_t total_file_count);

public:
	JSONScanGlobalState state;
};

struct JSONLocalTableFunctionState : public LocalTableFunctionState {
public:
	JSONLocalTableFunctionState(ClientContext &context, JSONScanGlobalState &gstate);

public:
	JSONScanLocalState state;
};

struct JSONScan {
public:
	//! Parse a single read_json option - returns false if the option is not a JSON reader option
	static bool ParseOption(ClientContext &context, const Identifier &key, const Value &value,
	                        JSONReaderOptions &options);

	//! Determine the names/types that are read from the given set of files - performing auto-detection if required.
	//! Readers that were opened during auto-detection are stored in "union_readers" so they can be re-used
	static void BindSchema(ClientContext &context, JSONScanData &json_data, MultiFileList &files,
	                       vector<shared_ptr<BaseUnionData>> &union_readers, bool union_by_name,
	                       vector<LogicalType> &return_types, vector<Identifier> &names);
	//! Set up the transform options and de-duplicate the (case-insensitively) colliding column names
	static void FinalizeBind(JSONScanData &json_data, vector<Identifier> &names);

	static void AutoDetect(ClientContext &context, JSONScanData &json_data, const vector<OpenFileInfo> &files,
	                       vector<shared_ptr<BaseUnionData>> &union_readers, bool union_by_name,
	                       vector<LogicalType> &return_types, vector<Identifier> &names);

	static void Serialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
	                      const TableFunction &function);
	static unique_ptr<FunctionData> Deserialize(Deserializer &deserializer, TableFunction &function);

	static void TableFunctionDefaults(TableFunction &table_function);
	//! The named parameters shared by all read_json variants
	static void AddReadJSONParameters(TableFunction &table_function);
	//! The named parameters that steer the schema auto-detection
	static void AddAutoDetectParameters(TableFunction &table_function);
};

//! Read a chunk of rows from the given JSON reader (JSONScanType::READ_JSON)
void ReadJSONFunction(ClientContext &context, JSONReader &json_reader, JSONScanGlobalState &gstate,
                      JSONScanLocalState &lstate, DataChunk &output);
//! Read a chunk of unparsed JSON objects from the given JSON reader (JSONScanType::READ_JSON_OBJECTS)
void ReadJSONObjectsFunction(ClientContext &context, JSONReader &json_reader, JSONScanGlobalState &gstate,
                             JSONScanLocalState &lstate, DataChunk &output);

} // namespace duckdb
