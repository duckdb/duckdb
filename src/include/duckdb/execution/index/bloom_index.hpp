//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/bloom_index.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/bound_index.hpp"
#include "duckdb/execution/index/index_type.hpp"
#include "duckdb/storage/checkpoint/table_index_writer.hpp"
#include "duckdb/storage/statistics/bloom_statistics.hpp"

namespace duckdb {

class BloomIndex : public BoundIndex {
public:
	static constexpr const char *TYPE_NAME = "BLOOM";

	BloomIndex(const Identifier &name, IndexConstraintType index_constraint_type, const vector<column_t> &column_ids,
	           TableIOManager &table_io_manager, const vector<unique_ptr<Expression>> &unbound_expressions,
	           AttachedDatabase &db);

	vector<vector<BloomColumnFilter>> filters;

public:
	static unique_ptr<BoundIndex> Create(CreateIndexInput &input);
	static IndexType GetBloomIndexType();

	ErrorData Append(IndexLock &l, DataChunk &chunk, Vector &row_ids) DUCKDB_REQUIRES(l) final;
	ErrorData Insert(IndexLock &l, DataChunk &chunk, Vector &row_ids) DUCKDB_REQUIRES(l) final;
	bool MergeIndexes(IndexLock &state, BoundIndex &other_index) DUCKDB_REQUIRES(state) final;
	void ResetStorage(IndexLock &index_lock) DUCKDB_REQUIRES(index_lock) final;
	void Vacuum(IndexLock &l) DUCKDB_REQUIRES(l) final;
	idx_t GetInMemorySize(IndexLock &state) const DUCKDB_REQUIRES(state) final;
	void Verify(IndexLock &l) DUCKDB_REQUIRES(l) final;
	string ToString(IndexLock &l, bool display_ascii = false) DUCKDB_REQUIRES(l) final;
	void VerifyAllocations(IndexLock &l) DUCKDB_REQUIRES(l) final;
	string GetConstraintViolationMessage(VerifyExistenceType verify_type, idx_t failed_index, DataChunk &input) const
	    DUCKDB_EXCLUDES(lock) final;
	idx_t TryDelete(IndexLock &state, DataChunk &entries, Vector &row_identifiers,
	                optional_ptr<SelectionVector> deleted_sel = nullptr,
	                optional_ptr<SelectionVector> non_deleted_sel = nullptr) DUCKDB_REQUIRES(state) final;
	bool CanSkipVector(column_t column_id, idx_t granule, const Value &constant) const final;
	bool PrepareSkip(column_t column_id, const Value &constant, idx_t &column_index, hash_t &hash) const final;
	bool SkipPrepared(idx_t column_index, idx_t granule, hash_t hash) const final;
	IndexStorageInfo SerializeToWAL(StorageVersion storage_version) final;
	IndexCheckpointResult Checkpoint(PartialBlockManager &partial_block_manager, const StorageVersion version) override;

private:
	void LoadFilters(const string &bytes);
	bool IndexesPlainColumns() const;
};

} // namespace duckdb
