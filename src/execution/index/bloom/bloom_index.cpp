#include "duckdb/execution/index/bloom_index.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/execution/index/index_lock.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/index_storage_info.hpp"
#include "duckdb/storage/table_io_manager.hpp"

#include <cstring>

namespace duckdb {

BloomIndex::BloomIndex(const Identifier &name, const IndexConstraintType index_constraint_type,
                       const vector<column_t> &column_ids, TableIOManager &table_io_manager,
                       const vector<unique_ptr<Expression>> &unbound_expressions, AttachedDatabase &db)
    : BoundIndex(name, TYPE_NAME, index_constraint_type, column_ids, table_io_manager, unbound_expressions, db) {
}

class BloomBuildBindData : public IndexBuildBindData {
public:
	bool sorted = false;
};

unique_ptr<IndexBuildBindData> BloomBind(IndexBuildBindInput &input) {
	auto bind_data = make_uniq<BloomBuildBindData>();
	bind_data->sorted = false;

	return bind_data;
}

bool BloomBuildSort(IndexBuildSortInput &input) {
	auto &bind_data = input.bind_data->Cast<BloomBuildBindData>();
	return bind_data.sorted;
}

class BloomBuildGlobalState : public IndexBuildGlobalState {
public:
	unique_ptr<BoundIndex> global_index;
};

unique_ptr<IndexBuildGlobalState> BloomBuildGlobalInit(IndexBuildInitGlobalStateInput &input) {
	auto next_row_id = input.table.GetStorage().GetNextRowId();
	auto granule_count = (next_row_id + STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE;
	auto column_count = input.storage_ids.size();
	auto &storage = input.table.GetStorage();
	auto bloom_index = make_uniq<BloomIndex>(input.info.GetIndexName(), input.info.constraint_type, input.storage_ids,
	                                         TableIOManager::Get(storage), input.expressions, storage.db);
	bloom_index->filters.resize(column_count);
	for (idx_t i = 0; i < column_count; ++i) {
		bloom_index->filters[i].resize(granule_count);
	}
	auto state = make_uniq<BloomBuildGlobalState>();

	state->global_index = std::move(bloom_index);

	return state;
}

class BloomBuildLocalState : public IndexBuildLocalState {
public:
	explicit BloomBuildLocalState(ClientContext &) {
	}
};

unique_ptr<IndexBuildLocalState> BloomBuildLocalInit(IndexBuildInitLocalStateInput &input) {
	return make_uniq<BloomBuildLocalState>(input.context);
}

void BloomBuildSink(IndexBuildSinkInput &input, DataChunk &key_chunk, DataChunk &row_chunk) {
	auto count = key_chunk.size();
	if (count == 0) {
		return;
	}
	auto &gstate = input.global_state.Cast<BloomBuildGlobalState>();
	auto &bloom_index = gstate.global_index->Cast<BloomIndex>();
	auto row_ids = FlatVector::GetData<row_t>(row_chunk.data[0]);
	auto granule_idx = UnsafeNumericCast<idx_t>(row_ids[0] / STANDARD_VECTOR_SIZE);
	for (idx_t col = 0; col < key_chunk.ColumnCount(); ++col) {
		Vector hashes(LogicalType::HASH, count);
		VectorOperations::Hash(key_chunk.data[col], hashes, count);
		if (granule_idx >= bloom_index.filters[col].size()) {
			bloom_index.filters[col].resize(granule_idx + 1);
		}
		bloom_index.filters[col][granule_idx].InsertHashes(hashes, count);
	}
}

void BloomBuildCombine(IndexBuildCombineInput &input) {
}

unique_ptr<BoundIndex> BloomBuildFinalize(IndexBuildFinalizeInput &input) {
	auto &gstate = input.global_state.Cast<BloomBuildGlobalState>();
	return std::move(gstate.global_index);
}

unique_ptr<BoundIndex> BloomIndex::Create(CreateIndexInput &input) {
	auto bloom = make_uniq<BloomIndex>(input.name, input.constraint_type, input.column_ids, input.table_io_manager,
	                                   input.unbound_expressions, input.db);
	auto stored = input.storage_info.options.find("bloom");
	if (stored != input.storage_info.options.end() && !stored->second.IsNull()) {
		bloom->LoadFilters(StringValue::Get(stored->second));
	}
	return bloom;
}

IndexType BloomIndex::GetBloomIndexType() {
	IndexType bloom_index_type;
	bloom_index_type.name = BloomIndex::TYPE_NAME;
	bloom_index_type.create_instance = BloomIndex::Create;
	bloom_index_type.build_bind = BloomBind;
	bloom_index_type.build_sort = BloomBuildSort;
	bloom_index_type.build_global_init = BloomBuildGlobalInit;
	bloom_index_type.build_local_init = BloomBuildLocalInit;
	bloom_index_type.build_sink = BloomBuildSink;
	bloom_index_type.build_combine = BloomBuildCombine;
	bloom_index_type.build_finalize = BloomBuildFinalize;
	return bloom_index_type;
}

ErrorData BloomIndex::Append(IndexLock &l, DataChunk &chunk, Vector &row_ids) {
	l.AssertHeld(*this);
	DataChunk keys;
	keys.Initialize(Allocator::DefaultAllocator(), logical_types);
	ExecuteExpressions(chunk, keys);
	auto count = keys.size();
	row_ids.Flatten();
	auto ids = FlatVector::GetData<row_t>(row_ids);
	for (idx_t col = 0; col < keys.ColumnCount(); col++) {
		Vector hashes(LogicalType::HASH, count);
		VectorOperations::Hash(keys.data[col], hashes, count);
		auto hash_data = FlatVector::GetData<hash_t>(hashes);
		for (idx_t row = 0; row < count; row++) {
			if (FlatVector::IsNull(keys.data[col], row)) {
				continue;
			}
			auto granule = UnsafeNumericCast<idx_t>(ids[row]) / STANDARD_VECTOR_SIZE;
			if (granule >= filters[col].size()) {
				filters[col].resize(granule + 1);
			}
			filters[col][granule].InsertOne(hash_data[row]);
		}
	}
	return ErrorData();
}

ErrorData BloomIndex::Insert(IndexLock &l, DataChunk &chunk, Vector &row_ids) {
	return Append(l, chunk, row_ids);
}

void BloomIndex::ResetStorage(IndexLock &index_lock) {
	index_lock.AssertHeld(*this);
	for (idx_t column_idx = 0; column_idx < filters.size(); column_idx++) {
		for (idx_t granule_idx = 0; granule_idx < filters[column_idx].size(); granule_idx++) {
			filters[column_idx][granule_idx].Clear();
		}
	}
}

bool BloomIndex::MergeIndexes(IndexLock &state, BoundIndex &) {
	state.AssertHeld(*this);
	// Each build thread writes its own granules into the shared filters.
	return true;
}

idx_t BloomIndex::TryDelete(IndexLock &state, DataChunk &entries, Vector &, optional_ptr<SelectionVector>,
                            optional_ptr<SelectionVector>) {
	state.AssertHeld(*this);
	// A cleared bit could hide another live row that shares it.
	return entries.size();
}

void BloomIndex::Vacuum(IndexLock &l) {
	l.AssertHeld(*this);
}

idx_t BloomIndex::GetInMemorySize(IndexLock &state) const DUCKDB_REQUIRES(state) {
	state.AssertHeld(*this);
	idx_t bytes = 0;
	for (auto &column_filters : filters) {
		bytes += column_filters.size() * sizeof(BloomColumnFilter);
	}
	return bytes;
}

void BloomIndex::Verify(IndexLock &l) {
	l.AssertHeld(*this);
}

string BloomIndex::ToString(IndexLock &l, bool) {
	l.AssertHeld(*this);
	return "BLOOM";
}

void BloomIndex::VerifyAllocations(IndexLock &l) {
	l.AssertHeld(*this);
}

string BloomIndex::GetConstraintViolationMessage(VerifyExistenceType, idx_t, DataChunk &) const {
	return "NOT_UNIQUE_BLOOM";
}

bool BloomIndex::IndexesPlainColumns() const {
	for (auto &expr : unbound_expressions) {
		if (!expr || expr->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			return false;
		}
	}
	return !unbound_expressions.empty();
}

bool BloomIndex::PrepareSkip(column_t column_id, const Value &constant, idx_t &column_index, hash_t &hash) const {
	if (constant.IsNull() || !IndexesPlainColumns()) {
		return false;
	}
	optional_idx found;
	for (idx_t i = 0; i < column_ids.size(); i++) {
		if (column_ids[i] == column_id) {
			found = i;
			break;
		}
	}
	if (!found.IsValid() || found.GetIndex() >= filters.size()) {
		return false;
	}
	Vector input(constant, count_t(1));
	Vector hashes(LogicalType::HASH, 1);
	VectorOperations::Hash(input, hashes, 1);
	column_index = found.GetIndex();
	hash = FlatVector::GetData<hash_t>(hashes)[0];
	return true;
}

bool BloomIndex::SkipPrepared(idx_t column_index, idx_t granule, hash_t hash) const {
	if (column_index >= filters.size()) {
		return false;
	}
	auto &column_filters = filters[column_index];
	if (granule >= column_filters.size()) {
		return false;
	}
	return !column_filters[granule].MaybeContainsOne(hash);
}

bool BloomIndex::CanSkipVector(column_t column_id, idx_t granule, const Value &constant) const {
	IndexLock guard(*this);
	idx_t column_index;
	hash_t hash;
	if (!PrepareSkip(column_id, constant, column_index, hash)) {
		return false;
	}
	return SkipPrepared(column_index, granule, hash);
}

static void AppendBytes(string &out, const void *data, idx_t size) {
	auto offset = out.size();
	out.resize(offset + size);
	memcpy(out.data() + offset, data, size);
}

void BloomIndex::LoadFilters(const string &bytes) {
	const auto *ptr = reinterpret_cast<const unsigned char *>(bytes.data());
	idx_t left = bytes.size();
	auto read = [&](void *dest, idx_t size) {
		if (left < size) {
			throw InternalException("Corrupt bloom index storage");
		}
		memcpy(dest, ptr, size);
		ptr += size;
		left -= size;
	};

	uint32_t column_count = 0;
	read(&column_count, sizeof(column_count));
	if (column_count != column_ids.size()) {
		throw InternalException("Bloom index column count does not match the index");
	}
	filters.clear();
	filters.resize(column_count);
	for (idx_t column = 0; column < column_count; column++) {
		uint64_t granule_count = 0;
		read(&granule_count, sizeof(granule_count));
		filters[column].resize(NumericCast<idx_t>(granule_count));
		for (idx_t granule = 0; granule < filters[column].size(); granule++) {
			read(filters[column][granule].words.data(), filters[column][granule].words.size() * sizeof(uint64_t));
		}
	}
	if (left != 0) {
		throw InternalException("Corrupt bloom index storage");
	}
}

IndexStorageInfo BloomIndex::SerializeToWAL(StorageVersion) {
	IndexLock guard(*this);
	string bytes;
	auto column_count = NumericCast<uint32_t>(filters.size());
	AppendBytes(bytes, &column_count, sizeof(column_count));
	for (auto &column_filters : filters) {
		auto granule_count = NumericCast<uint64_t>(column_filters.size());
		AppendBytes(bytes, &granule_count, sizeof(granule_count));
		for (auto &filter : column_filters) {
			AppendBytes(bytes, filter.words.data(), filter.words.size() * sizeof(uint64_t));
		}
	}

	IndexStorageInfo info(name);
	info.options["bloom"] = Value::BLOB(const_data_ptr_cast(bytes.data()), bytes.size());
	info.allocator_infos.emplace_back();
	return info;
}

IndexCheckpointResult BloomIndex::Checkpoint(PartialBlockManager &, const StorageVersion version) {
	auto info = SerializeToWAL(version);
	return {make_shared_ptr<IndexStorageInfo>(std::move(info)), nullptr};
}

} // namespace duckdb
