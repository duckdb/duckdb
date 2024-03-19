//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/unknown_index.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/index.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"

namespace duckdb {

// An unknown index is an index that has been created by an extension, which has not been loaded yet.
// It is used as a placeholder for the index until the extension is loaded, at which point the extension will replace
// all recognized unknown indexes with the correct index type.
// Calling any function on an unknown index will throw a NotImplementedException
class UnknownIndex final : public Index {
private:
	CreateIndexInfo create_info;
	IndexStorageInfo storage_info;
	string GenerateErrorMessage() const;

public:
	UnknownIndex(const string &name, const string &index_type, IndexConstraintType index_constraint_type,
	             const vector<column_t> &column_ids, TableIOManager &table_io_manager,
	             const vector<unique_ptr<Expression>> &unbound_expressions, AttachedDatabase &db,
	             const CreateIndexInfo &create_info, IndexStorageInfo storage_info);

	const CreateIndexInfo &GetCreateInfo() const {
		return create_info;
	}
	const IndexStorageInfo &GetStorageInfo() const {
		return storage_info;
	}
	const string &GetIndexType() {
		return create_info.index_type;
	}

public:
	bool IsUnknown() override {
		return true;
	}

	// Index interface (unused)

	ErrorData Append(IndexLock &lock, DataChunk &entries, Vector &row_identifiers) override;
	void VerifyAppend(DataChunk &chunk) override;
	void VerifyAppend(DataChunk &chunk, ConflictManager &conflict_manager) override;
	void CommitDrop(IndexLock &index_lock) override;
	void Delete(IndexLock &lock, DataChunk &entries, Vector &row_identifiers) override;
	ErrorData Insert(IndexLock &lock, DataChunk &data, Vector &row_ids) override;
	IndexStorageInfo GetStorageInfo(bool get_buffers) override;
	bool MergeIndexes(IndexLock &state, Index &other_index) override;
	void Vacuum(IndexLock &state) override;
	idx_t GetInMemorySize(IndexLock &index_lock) override;
	void CheckConstraintsForChunk(DataChunk &input, ConflictManager &conflict_manager) override;
	string VerifyAndToString(IndexLock &state, bool only_verify) override;
	string GetConstraintViolationMessage(VerifyExistenceType verify_type, idx_t failed_index,
	                                     DataChunk &input) override;
};

} // namespace duckdb
