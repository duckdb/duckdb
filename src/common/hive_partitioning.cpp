#include "duckdb/common/hive_partitioning.hpp"

#include "duckdb/common/uhugeint.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/main/error_manager.hpp"
#include "utf8proc_wrapper.hpp"

namespace duckdb {

struct PartitioningColumnValue {
	explicit PartitioningColumnValue(string value_p) : value(std::move(value_p)) {
	}
	PartitioningColumnValue(string key_p, string value_p) : key(std::move(key_p)), value(std::move(value_p)) {
	}

	string key;
	string value;
};

static unordered_map<ProjectionIndex, PartitioningColumnValue>
GetKnownColumnValues(const string &filename, const HivePartitioningFilterInfo &filter_info) {
	unordered_map<ProjectionIndex, PartitioningColumnValue> result;

	auto &column_map = filter_info.column_map;
	if (filter_info.filename_enabled) {
		auto lookup_column_id = column_map.find(filter_info.filename_column);
		if (lookup_column_id != column_map.end()) {
			result.insert(make_pair(lookup_column_id->second, PartitioningColumnValue(filename)));
		}
	}

	if (filter_info.hive_enabled) {
		auto partitions = HivePartitioning::Parse(filename);
		for (auto &partition : partitions) {
			auto lookup_column_id = column_map.find(Identifier(partition.first));
			if (lookup_column_id != column_map.end()) {
				result.insert(
				    make_pair(lookup_column_id->second, PartitioningColumnValue(partition.first, partition.second)));
			}
		}
	}

	return result;
}

//! Unescapes a partition value as a string of the given type - returns false if the value is NULL
static bool TryGetPartitionString(const string &str_val, const LogicalType &type, string &result) {
	// On SQLNULL, DuckDB writes "__HIVE_DEFAULT_PARTITION__", instead of string version "NULL".
	if (str_val == HivePartitioning::DEFAULT_PARTITION_NAME) {
		return false;
	}
	// "NULL" and empty strings are NULL for non-string partition types
	if (type.id() != LogicalTypeId::VARCHAR && (StringUtil::CIEquals(str_val, "NULL") || str_val.empty())) {
		return false;
	}
	result = HivePartitioning::Unescape(str_val);
	return true;
}

// Replaces references to columns of the given table using the replacement callback (nullptr = keep)
static void ReplaceTableColumns(unique_ptr<Expression> &expr, TableIndex table_index,
                                const std::function<unique_ptr<Expression>(BoundColumnRefExpression &)> &replace) {
	if (expr->GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
		auto &bound_colref = expr->Cast<BoundColumnRefExpression>();
		if (bound_colref.Depth() != 0 || bound_colref.Binding().table_index != table_index) {
			return;
		}
		auto replacement = replace(bound_colref);
		if (replacement) {
			expr = std::move(replacement);
		}
		return;
	}
	ExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<Expression> &child) { ReplaceTableColumns(child, table_index, replace); });
}

enum class PartitionFilterResult : uint8_t { NOT_EVALUATED, KEEP_FILE, PRUNE_FILE };

//! A filter rewritten to read the partition columns it references from an input chunk
struct PartitionFilter {
	unique_ptr<Expression> expr;
	vector<ProjectionIndex> columns;
	vector<LogicalType> types;
};

static unique_ptr<PartitionFilter> PreparePartitionFilter(ClientContext &context, const Expression &filter,
                                                          const HivePartitioningFilterInfo &filter_info,
                                                          TableIndex table_index) {
	unordered_set<ProjectionIndex> partition_columns;
	for (auto &entry : filter_info.column_map) {
		if (filter_info.hive_enabled || (filter_info.filename_enabled && entry.first == filter_info.filename_column)) {
			partition_columns.insert(ProjectionIndex(entry.second));
		}
	}

	// the filter can only be evaluated if it becomes foldable once the partition columns are known
	auto check = filter.Copy();
	ReplaceTableColumns(check, table_index, [&](BoundColumnRefExpression &colref) -> unique_ptr<Expression> {
		if (partition_columns.find(colref.Binding().column_index) == partition_columns.end()) {
			return nullptr;
		}
		return make_uniq<BoundConstantExpression>(Value(colref.GetReturnType()));
	});
	if (!check->IsScalar() || !check->IsFoldable()) {
		return nullptr;
	}

	auto result = make_uniq<PartitionFilter>();
	result->expr = filter.Copy();
	ReplaceTableColumns(result->expr, table_index, [&](BoundColumnRefExpression &colref) {
		auto column_index = colref.Binding().column_index;
		idx_t slot;
		for (slot = 0; slot < result->columns.size(); slot++) {
			if (result->columns[slot] == column_index) {
				break;
			}
		}
		if (slot == result->columns.size()) {
			result->columns.push_back(column_index);
			result->types.push_back(colref.GetReturnType());
		}
		// the partition values are loaded as strings and cast as part of the filter
		auto reference = make_uniq<BoundReferenceExpression>(LogicalType::VARCHAR, slot);
		return BoundCastExpression::AddCastToType(context, std::move(reference), colref.GetReturnType());
	});
	return result;
}

//! Evaluates the filter over a chunk of files - if the filter throws, the files in the chunk are not pruned
static void EvaluatePartitionFilterChunk(ExpressionExecutor &executor, DataChunk &chunk,
                                         array_ptr<const idx_t> file_indexes,
                                         vector<PartitionFilterResult> &file_results) {
	SelectionVector true_sel(chunk.size());
	idx_t true_count;
	try {
		true_count = executor.SelectExpression(chunk, true_sel);
	} catch (InternalException &) {
		throw;
	} catch (...) {
		return;
	}
	for (auto &file_idx : file_indexes) {
		file_results[file_idx] = PartitionFilterResult::PRUNE_FILE;
	}
	for (idx_t i = 0; i < true_count; i++) {
		file_results[file_indexes[true_sel.get_index(i)]] = PartitionFilterResult::KEEP_FILE;
	}
}

//! Loads the (unescaped) string values of a partition column for the given files into the result vector
static void LoadPartitionColumn(ProjectionIndex column, const LogicalType &type,
                                const vector<unordered_map<ProjectionIndex, PartitioningColumnValue>> &known_values,
                                array_ptr<const idx_t> file_indexes, Vector &result) {
	auto count = file_indexes.size();
	Vector strings(LogicalType::VARCHAR, count);
	{
		auto writer = FlatVector::Writer<string_t>(strings, count);
		string unescaped;
		for (auto &file_idx : file_indexes) {
			auto &partition_val = known_values[file_idx].find(column)->second;
			if (partition_val.key.empty()) {
				// filename column - use directly
				unescaped = partition_val.value;
			} else if (!TryGetPartitionString(partition_val.value, type, unescaped)) {
				writer.WriteNull();
				continue;
			}
			if (!Utf8Proc::IsValid(unescaped.c_str(), unescaped.size())) {
				throw ErrorManager::InvalidUnicodeError(unescaped, "value construction");
			}
			writer.WriteValue(string_t(unescaped));
		}
	}
	result.Reference(strings);
}

static vector<PartitionFilterResult>
EvaluatePartitionFilter(ClientContext &context, const PartitionFilter &filter,
                        const vector<unordered_map<ProjectionIndex, PartitioningColumnValue>> &known_values) {
	vector<PartitionFilterResult> file_results(known_values.size(), PartitionFilterResult::NOT_EVALUATED);
	// files that have values for all partition columns referenced by the filter
	vector<idx_t> file_indexes;
	for (idx_t file_idx = 0; file_idx < known_values.size(); file_idx++) {
		auto &file_values = known_values[file_idx];
		bool has_all_columns = true;
		for (auto &column : filter.columns) {
			if (file_values.find(column) == file_values.end()) {
				has_all_columns = false;
				break;
			}
		}
		if (has_all_columns) {
			file_indexes.push_back(file_idx);
		}
	}

	ExpressionExecutor executor(context, *filter.expr);
	DataChunk chunk;
	chunk.InitializeEmpty(vector<LogicalType>(filter.columns.size(), LogicalType::VARCHAR));
	for (idx_t offset = 0; offset < file_indexes.size(); offset += STANDARD_VECTOR_SIZE) {
		auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, file_indexes.size() - offset);
		auto batch = array_ptr<const idx_t>(file_indexes.data() + offset, count);
		for (idx_t col = 0; col < filter.columns.size(); col++) {
			LoadPartitionColumn(filter.columns[col], filter.types[col], known_values, batch, chunk.data[col]);
		}
		chunk.SetChildCardinality(count);
		EvaluatePartitionFilterChunk(executor, chunk, batch, file_results);
	}
	return file_results;
}

string HivePartitioning::Escape(const string &input) {
	return StringUtil::URLEncode(input);
}

string HivePartitioning::EscapeValue(const string &input) {
	auto result = Escape(input);
	// the comparison is case-insensitive because on a case-insensitive file system a value that differs only in
	// case still lands in the directory that is reserved for NULL values
	if (!StringUtil::CIEquals(result, DEFAULT_PARTITION_NAME)) {
		return result;
	}
	// percent-encode the first character so the value gets its own directory while still unescaping back to the
	// original value
	static constexpr const char *HEX_DIGIT = "0123456789ABCDEF";
	const auto first = static_cast<unsigned char>(result[0]);
	string escaped = "%";
	escaped += HEX_DIGIT[first >> 4];
	escaped += HEX_DIGIT[first & 15];
	escaped += result.substr(1);
	return escaped;
}

string HivePartitioning::Unescape(const string &input) {
	return StringUtil::URLDecode(input);
}

bool HivePartitioning::IsNull(const string &input) {
	return StringUtil::CIEquals(input, "NULL") || input == DEFAULT_PARTITION_NAME;
}

// matches hive partitions in file name. For example:
// 	- s3://bucket/var1=value1/bla/bla/var2=value2
//  - http(s)://domain(:port)/lala/kasdl/var1=value1/?not-a-var=not-a-value
//  - folder/folder/folder/../var1=value1/etc/.//var2=value2
std::map<string, string> HivePartitioning::Parse(const string &filename) {
	idx_t partition_start = 0;
	idx_t equality_sign = 0;
	bool candidate_partition = true;
	std::map<string, string> result;
	for (idx_t c = 0; c < filename.size(); c++) {
		if (filename[c] == '?' || filename[c] == '\n') {
			// get parameter or newline - not a partition
			candidate_partition = false;
		}
		if (filename[c] == '\\' || filename[c] == '/') {
			// separator
			if (candidate_partition && equality_sign > partition_start) {
				// we found a partition with an equality sign
				string key = filename.substr(partition_start, equality_sign - partition_start);
				string value = filename.substr(equality_sign + 1, c - equality_sign - 1);
				result.insert(make_pair(std::move(key), std::move(value)));
			}
			partition_start = c + 1;
			candidate_partition = true;
		} else if (filename[c] == '=') {
			if (equality_sign > partition_start) {
				// multiple equality signs - not a partition
				candidate_partition = false;
			}
			equality_sign = c;
		}
	}
	return result;
}

Value HivePartitioning::GetValue(ClientContext &context, const string &key, const string &str_val,
                                 const LogicalType &type) {
	string unescaped;
	if (!TryGetPartitionString(str_val, type, unescaped)) {
		return Value(type);
	}
	Value value(std::move(unescaped));
	if (type.id() == LogicalTypeId::VARCHAR) {
		return value;
	}
	auto cast = value.TryCastAs(context, type);
	if (!cast) {
		throw InvalidInputException("Unable to cast '%s' (from hive partition column '%s') to: '%s'", value.ToString(),
		                            StringUtil::Upper(key), type.ToString());
	}
	return std::move(*cast);
}

// TODO: this can still be improved by removing the parts of filter expressions that are true for all remaining files.
//		 currently, only expressions that cannot be evaluated during pushdown are removed.
void HivePartitioning::ApplyFiltersToFileList(ClientContext &context, vector<OpenFileInfo> &files,
                                              vector<unique_ptr<Expression>> &filters,
                                              const HivePartitioningFilterInfo &filter_info,
                                              MultiFilePushdownInfo &info) {
	vector<OpenFileInfo> pruned_files;
	vector<bool> have_preserved_filter(filters.size(), false);
	vector<unique_ptr<Expression>> pruned_filters;
	unordered_set<idx_t> filters_applied_to_files;
	auto table_index = info.table_index;

	if ((!filter_info.filename_enabled && !filter_info.hive_enabled) || filters.empty()) {
		return;
	}

	if (!info.extra_info.total_files.IsValid()) {
		info.extra_info.file_filter_expressions = vector<unique_ptr<Expression>>();
	}

	vector<unordered_map<ProjectionIndex, PartitioningColumnValue>> known_values;
	known_values.reserve(files.size());
	for (auto &file : files) {
		known_values.push_back(GetKnownColumnValues(file.path, filter_info));
	}
	vector<vector<PartitionFilterResult>> filter_results;
	for (auto &filter : filters) {
		auto partition_filter = PreparePartitionFilter(context, *filter, filter_info, table_index);
		if (!partition_filter) {
			filter_results.emplace_back(files.size(), PartitionFilterResult::NOT_EVALUATED);
			continue;
		}
		filter_results.push_back(EvaluatePartitionFilter(context, *partition_filter, known_values));
	}

	for (idx_t i = 0; i < files.size(); i++) {
		bool should_prune_file = false;
		for (idx_t j = 0; j < filters.size(); j++) {
			auto &filter = filters[j];
			switch (filter_results[j][i]) {
			case PartitionFilterResult::NOT_EVALUATED:
				// can not be evaluated only with the filename/hive columns added, we can not prune this filter
				if (!have_preserved_filter[j]) {
					pruned_filters.emplace_back(filter->Copy());
					have_preserved_filter[j] = true;
				}
				break;
			case PartitionFilterResult::PRUNE_FILE:
				should_prune_file = true;
				if (filters_applied_to_files.find(j) == filters_applied_to_files.end()) {
					info.extra_info.file_filters += filter->ToString();
					filters_applied_to_files.insert(j);
				}
				break;
			case PartitionFilterResult::KEEP_FILE:
				break;
			}
		}

		if (!should_prune_file) {
			pruned_files.push_back(files[i]);
		}
	}

	D_ASSERT(filters.size() >= pruned_filters.size());

	for (idx_t i = 0; i < filters.size() && info.extra_info.file_filter_expressions; i++) {
		if (have_preserved_filter[i]) {
			continue;
		}
		auto retained = filters[i]->Copy();
		bool representable = true;
		retained = ExpressionIterator::PreorderReplace<BoundColumnRefExpression>(
		    std::move(retained), [&](const BoundColumnRefExpression &column) -> unique_ptr<Expression> {
			    auto binding = column.Binding();
			    if (column.Depth() != 0 || binding.table_index != table_index ||
			        binding.column_index.GetIndex() >= info.column_indexes.size()) {
				    representable = false;
				    return nullptr;
			    }
			    auto &index = info.column_indexes[binding.column_index.GetIndex()];
			    if (index.IsVirtualColumn() || index.IsPushdownExtract() || index.HasType()) {
				    representable = false;
				    return nullptr;
			    }
			    return column.WithBinding(ColumnBinding(TableIndex(ExtraOperatorInfo::FILE_FILTER_TABLE_INDEX),
			                                            ProjectionIndex(index.GetPrimaryIndex())));
		    });
		if (!representable) {
			info.extra_info.file_filter_expressions.reset();
			break;
		}
		info.extra_info.file_filter_expressions->push_back(std::move(retained));
	}

	info.extra_info.total_files = files.size();
	info.extra_info.filtered_files = pruned_files.size();

	filters = std::move(pruned_filters);
	files = std::move(pruned_files);
}

void HivePartitionedColumnData::InitializeKeys() {
	keys.resize(STANDARD_VECTOR_SIZE);
	for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
		keys[i].values.resize(group_by_columns.size());
	}
}

template <class T>
static inline Value GetHiveKeyValue(const T &val) {
	return Value::CreateValue<T>(val);
}

template <class T>
static inline Value GetHiveKeyValue(const T &val, const LogicalType &type) {
	return GetHiveKeyValue(val).WithType(type);
}

static inline Value GetHiveKeyNullValue(const LogicalType &type) {
	return Value().WithType(type);
}

template <class T>
static void TemplatedGetHivePartitionValues(const Vector &input, vector<HivePartitionKey> &keys, const idx_t col_idx) {
	auto entries = input.Values<T>();
	const auto &type = input.GetType();

	for (idx_t i = 0; i < input.size(); i++) {
		auto &key = keys[i];
		auto entry = entries[i];
		if (entry.IsValid()) {
			key.values[col_idx] = GetHiveKeyValue(entry.GetValue(), type);
		} else {
			key.values[col_idx] = GetHiveKeyNullValue(type);
		}
	}
}

static void GetNestedHivePartitionValues(const Vector &input, vector<HivePartitionKey> &keys, const idx_t col_idx) {
	for (idx_t i = 0; i < input.size(); i++) {
		auto &key = keys[i];
		key.values[col_idx] = input.GetValue(i);
	}
}

static void GetHivePartitionValuesTypeSwitch(const Vector &input, vector<HivePartitionKey> &keys, const idx_t col_idx) {
	const auto &type = input.GetType();
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		TemplatedGetHivePartitionValues<bool>(input, keys, col_idx);
		break;
	case PhysicalType::INT8:
		TemplatedGetHivePartitionValues<int8_t>(input, keys, col_idx);
		break;
	case PhysicalType::INT16:
		TemplatedGetHivePartitionValues<int16_t>(input, keys, col_idx);
		break;
	case PhysicalType::INT32:
		TemplatedGetHivePartitionValues<int32_t>(input, keys, col_idx);
		break;
	case PhysicalType::INT64:
		TemplatedGetHivePartitionValues<int64_t>(input, keys, col_idx);
		break;
	case PhysicalType::INT128:
		TemplatedGetHivePartitionValues<hugeint_t>(input, keys, col_idx);
		break;
	case PhysicalType::UINT8:
		TemplatedGetHivePartitionValues<uint8_t>(input, keys, col_idx);
		break;
	case PhysicalType::UINT16:
		TemplatedGetHivePartitionValues<uint16_t>(input, keys, col_idx);
		break;
	case PhysicalType::UINT32:
		TemplatedGetHivePartitionValues<uint32_t>(input, keys, col_idx);
		break;
	case PhysicalType::UINT64:
		TemplatedGetHivePartitionValues<uint64_t>(input, keys, col_idx);
		break;
	case PhysicalType::UINT128:
		TemplatedGetHivePartitionValues<uhugeint_t>(input, keys, col_idx);
		break;
	case PhysicalType::FLOAT:
		TemplatedGetHivePartitionValues<float>(input, keys, col_idx);
		break;
	case PhysicalType::DOUBLE:
		TemplatedGetHivePartitionValues<double>(input, keys, col_idx);
		break;
	case PhysicalType::INTERVAL:
		TemplatedGetHivePartitionValues<interval_t>(input, keys, col_idx);
		break;
	case PhysicalType::VARCHAR:
		TemplatedGetHivePartitionValues<string_t>(input, keys, col_idx);
		break;
	case PhysicalType::STRUCT:
	case PhysicalType::LIST:
		GetNestedHivePartitionValues(input, keys, col_idx);
		break;
	default:
		throw InternalException("Unsupported type for HivePartitionedColumnData::ComputePartitionIndices");
	}
}

void HivePartitionedColumnData::ComputePartitionIndices(PartitionedColumnDataAppendState &state, DataChunk &input) {
	const auto count = input.size();

	input.Hash(group_by_columns, hashes_v);
	hashes_v.Flatten();

	for (idx_t col_idx = 0; col_idx < group_by_columns.size(); col_idx++) {
		const auto &group_by_col = input.data[group_by_columns[col_idx]];
		GetHivePartitionValuesTypeSwitch(group_by_col, keys, col_idx);
	}

	const auto hashes = FlatVector::GetData<hash_t>(hashes_v);
	auto partition_indices = FlatVector::Writer<idx_t>(state.partition_indices, count);
	for (idx_t i = 0; i < count; i++) {
		auto &key = keys[i];
		key.hash = hashes[i];
		auto lookup = local_partition_map.find(key);
		if (lookup == local_partition_map.end()) {
			idx_t new_partition_id = RegisterNewPartition(key, state);
			partition_indices.WriteValue(new_partition_id);
		} else {
			partition_indices.WriteValue(lookup->second);
		}
	}
}

std::map<idx_t, const HivePartitionKey *> HivePartitionedColumnData::GetReverseMap() {
	std::map<idx_t, const HivePartitionKey *> ret;
	for (const auto &pair : local_partition_map) {
		ret[pair.second] = &(pair.first);
	}
	return ret;
}

HivePartitionedColumnData::HivePartitionedColumnData(ClientContext &context, vector<LogicalType> types,
                                                     vector<idx_t> partition_by_cols,
                                                     shared_ptr<GlobalHivePartitionState> global_state)
    : PartitionedColumnData(PartitionedColumnDataType::HIVE, context, std::move(types)),
      global_state(std::move(global_state)), group_by_columns(std::move(partition_by_cols)),
      hashes_v(LogicalType::HASH) {
	InitializeKeys();
	CreateAllocator();
}

void HivePartitionedColumnData::AddNewPartition(HivePartitionKey key, idx_t partition_id,
                                                PartitionedColumnDataAppendState &state) {
	local_partition_map.emplace(std::move(key), partition_id);

	if (state.partition_append_states.size() <= partition_id) {
		state.partition_append_states.resize(partition_id + 1);
		state.partition_buffers.resize(partition_id + 1);
		partitions.resize(partition_id + 1);
	}
	state.partition_append_states[partition_id] = make_uniq<ColumnDataAppendState>();
	state.partition_buffers[partition_id] = CreatePartitionBuffer();
	partitions[partition_id] = CreatePartitionCollection(0);
	partitions[partition_id]->InitializeAppend(*state.partition_append_states[partition_id]);
}

idx_t HivePartitionedColumnData::RegisterNewPartition(HivePartitionKey key, PartitionedColumnDataAppendState &state) {
	idx_t partition_id;
	if (global_state) {
		// Synchronize Global state with our local state with the newly discovered partition
		unique_lock<mutex> lck_gstate(global_state->lock);

		// Insert into global map, or return partition if already present
		auto res = global_state->partition_map.emplace(std::make_pair(key, global_state->partition_map.size()));
		partition_id = res.first->second;
	} else {
		partition_id = local_partition_map.size();
	}
	AddNewPartition(std::move(key), partition_id, state);
	return partition_id;
}

} // namespace duckdb
