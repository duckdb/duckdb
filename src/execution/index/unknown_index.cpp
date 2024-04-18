#include "duckdb/execution/index/unknown_index.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"

namespace duckdb {

//-------------------------------------------------------------------------------
// Unknown index
//-------------------------------------------------------------------------------

UnknownIndex::UnknownIndex(const string &name, const string &index_type, IndexConstraintType index_constraint_type,
                           const vector<column_t> &column_ids, TableIOManager &table_io_manager,
                           const vector<unique_ptr<Expression>> &unbound_expressions, AttachedDatabase &db,
                           const CreateIndexInfo &create_info_p, IndexStorageInfo storage_info_p)
    : Index(name, index_type, index_constraint_type, column_ids, table_io_manager, unbound_expressions, db),
      create_info(create_info_p), storage_info(std::move(storage_info_p)) {
}

string UnknownIndex::GenerateErrorMessage() const {
	return StringUtil::Format(
	    R"(Unknown index type "%s" for index "%s". You probably need to load an extension containing this index type)",
	    index_type.c_str(), name.c_str());
}

ErrorData UnknownIndex::Append(IndexLock &, DataChunk &, Vector &) {
	throw MissingExtensionException(GenerateErrorMessage());
}
void UnknownIndex::VerifyAppend(DataChunk &) {
	throw MissingExtensionException(GenerateErrorMessage());
}
void UnknownIndex::VerifyAppend(DataChunk &, ConflictManager &) {
	throw MissingExtensionException(GenerateErrorMessage());
}
void UnknownIndex::CommitDrop(IndexLock &) {
	throw MissingExtensionException(GenerateErrorMessage());
}
void UnknownIndex::Delete(IndexLock &, DataChunk &, Vector &) {
	throw MissingExtensionException(GenerateErrorMessage());
}
ErrorData UnknownIndex::Insert(IndexLock &, DataChunk &, Vector &) {
	throw MissingExtensionException(GenerateErrorMessage());
}
IndexStorageInfo UnknownIndex::GetStorageInfo(bool) {
	throw MissingExtensionException(GenerateErrorMessage());
}
bool UnknownIndex::MergeIndexes(IndexLock &, Index &) {
	throw MissingExtensionException(GenerateErrorMessage());
}
void UnknownIndex::Vacuum(IndexLock &) {
	throw MissingExtensionException(GenerateErrorMessage());
}
idx_t UnknownIndex::GetInMemorySize(IndexLock &) {
	throw MissingExtensionException(GenerateErrorMessage());
}
void UnknownIndex::CheckConstraintsForChunk(DataChunk &, ConflictManager &) {
	throw MissingExtensionException(GenerateErrorMessage());
}
string UnknownIndex::VerifyAndToString(IndexLock &, bool) {
	throw MissingExtensionException(GenerateErrorMessage());
}

string UnknownIndex::GetConstraintViolationMessage(VerifyExistenceType, idx_t, DataChunk &) {
	throw MissingExtensionException(GenerateErrorMessage());
}

} // namespace duckdb
