//===----------------------------------------------------------------------===//
//                         DuckDB
//
// main/result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/chunk_collection.hpp"

namespace duckdb {

class AbstractResult {
public:
	// FIXME the method names here are not consistent. Refactor plz.
	virtual bool GetSuccess() = 0;
	virtual const string &GetErrorMessage() = 0;
	virtual size_t column_count() = 0;
	virtual vector<TypeId> &types() = 0;
	virtual ~AbstractResult() {
	}

protected:
};

//! The result object holds the result of a query. It can either hold an error
//! message or a DataChunk that represents the return value of the column.
class DuckDBResult : public AbstractResult {
public:
	DuckDBResult();
	DuckDBResult(string error);

	const string &GetErrorMessage() override {
		return error;
	}

	bool GetSuccess() override {
		return success;
	}

	//! Returns the number of columns in the result
	size_t column_count() override {
		return collection.types.size();
	}
	//! Returns the number of elements in the result

	//! Returns a list of types of the result
	vector<TypeId> &types() override {
		return collection.types;
	}

	void Print();

	size_t size() {
		return collection.count;
	}

	//! Gets the value of the column at the given index. Note that this method
	//! is unsafe, it is up to the user to do (1) bounds checking (2) proper
	//! type checking. i.e. only use GetValue<int32_t> on columns of type
	//! TypeId::INTEGER
	template <class T> T GetValue(size_t column, size_t index) {
		auto &data = collection.GetChunk(index).data[column];
		auto offset_in_chunk = index % STANDARD_VECTOR_SIZE;
		return ((T *)data.data)[offset_in_chunk];
	}

	Value GetValue(size_t column, size_t index) {
		auto &data = collection.GetChunk(index).data[column];
		auto offset_in_chunk = index % STANDARD_VECTOR_SIZE;
		return data.GetValue(offset_in_chunk);
	}

	bool ValueIsNull(size_t column, size_t index) {
		auto &data = collection.GetChunk(index).data[column];
		auto offset_in_chunk = index % STANDARD_VECTOR_SIZE;
		return data.nullmask[offset_in_chunk];
	}

	bool Equals(DuckDBResult *other);

	//! The names of the result
	vector<string> names;
	ChunkCollection collection;
	bool success;

	string error;

private:
	DuckDBResult(const DuckDBResult &) = delete;
};

class ClientContext;

class DuckDBStreamingResult : public AbstractResult {
public:
	DuckDBStreamingResult(ClientContext &context) : context(context){};
	//	DuckDBStreamingResult(string error);

	bool GetSuccess() override;

	unique_ptr<DuckDBResult> Materialize(bool close = true);
	bool Close();
	unique_ptr<DataChunk> Fetch();

	size_t column_count() override;
	vector<TypeId> &types() override;
	const string &GetErrorMessage() override;

private:
	ClientContext &context;
};

} // namespace duckdb
