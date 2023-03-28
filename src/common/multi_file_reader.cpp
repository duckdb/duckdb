#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

void MultiFileReader::AddParameters(TableFunction &table_function) {
	table_function.named_parameters["filename"] = LogicalType::BOOLEAN;
	table_function.named_parameters["hive_partitioning"] = LogicalType::BOOLEAN;
	table_function.named_parameters["union_by_name"] = LogicalType::BOOLEAN;
}

vector<string> MultiFileReader::GetFileList(ClientContext &context, const Value &input, const string &name, FileGlobOptions options) {
	auto &config = DBConfig::GetConfig(context);
	if (!config.options.enable_external_access) {
		throw PermissionException("Scanning %s files is disabled through configuration", name);
	}
	if (input.IsNull()) {
		throw ParserException("%s reader cannot take NULL list as parameter", name);
	}
	FileSystem &fs = FileSystem::GetFileSystem(context);
	vector<string> files;
	if (input.type().id() == LogicalTypeId::VARCHAR) {
		auto file_name = StringValue::Get(input);
		files = fs.GlobFiles(file_name, context, options);
	} else if (input.type().id() == LogicalTypeId::LIST) {
		for (auto &val : ListValue::GetChildren(input)) {
			if (val.IsNull()) {
				throw ParserException("%s reader cannot take NULL input as parameter", name);
			}
			auto glob_files = fs.GlobFiles(StringValue::Get(val), context, options);
			files.insert(files.end(), glob_files.begin(), glob_files.end());
		}
	} else {
		throw InternalException("Unsupported type for MultiFileReader::GetFileList");
	}
	if (files.empty() && options == FileGlobOptions::DISALLOW_EMPTY) {
		throw IOException("%s reader needs at least one file to read", name);
	}
	return files;
}

bool MultiFileReader::ParseOption(const string &key, const Value &val, MultiFileReaderOptions &options) {
	auto loption = StringUtil::Lower(key);
	if (loption == "filename") {
		options.filename = BooleanValue::Get(val);
	} else if (loption == "hive_partitioning") {
		options.hive_partitioning = BooleanValue::Get(val);
	} else if (loption == "union_by_name") {
		options.union_by_name = BooleanValue::Get(val);
	} else {
		return false;
	}
	return true;
}

bool MultiFileReader::ParseCopyOption(const string &key, const vector<Value> &values, MultiFileReaderOptions &options) {
	auto loption = StringUtil::Lower(key);
	if (loption == "filename") {
		options.filename = true;
	} else if (loption == "hive_partitioning") {
		options.hive_partitioning = true;
	} else if (loption == "union_by_name") {
		options.union_by_name = true;
	} else {
		return false;
	}
	return true;
}

bool MultiFileReader::ComplexFilterPushdown(ClientContext &context, vector<string> &files, const MultiFileReaderOptions &options, LogicalGet &get, vector<unique_ptr<Expression>> &filters) {
	if (files.empty()) {
		return false;
	}
	if (!options.hive_partitioning && !options.filename) {
		return false;
	}
	auto &initial_filename = files[0];

	unordered_map<string, column_t> column_map;
	for (idx_t i = 0; i < get.column_ids.size(); i++) {
		column_map.insert({get.names[get.column_ids[i]], i});
	}

	HivePartitioning::ApplyFiltersToFileList(context, files, filters, column_map, get.table_index,
											 options.hive_partitioning,
											 options.filename);
	if (files.empty() || initial_filename != files[0]) {
		return true;
	}
	return false;
}

void MultiFileReaderOptions::Serialize(FieldWriter &writer) const {
	writer.WriteField<bool>(filename);
	writer.WriteField<bool>(hive_partitioning);
	writer.WriteField<bool>(union_by_name);
}

void MultiFileReaderOptions::Deserialize(FieldReader &reader) {
	filename = reader.ReadRequired<bool>();
	hive_partitioning = reader.ReadRequired<bool>();
	union_by_name = reader.ReadRequired<bool>();
}

void MultiFileReaderOptions::AddBatchInfo(BindInfo &bind_info) {
	bind_info.InsertOption("filename", Value::BOOLEAN(filename));
	bind_info.InsertOption("hive_partitioning", Value::BOOLEAN(hive_partitioning));
	bind_info.InsertOption("union_by_name", Value::BOOLEAN(union_by_name));
}

}
