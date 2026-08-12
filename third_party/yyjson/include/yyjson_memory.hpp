//===----------------------------------------------------------------------===//
//                         DuckDB
//
// yyjson_memory.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "yyjson.hpp"
#include "duckdb/common/optional.hpp"

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

class JSONAllocator;

class JSONStringVectorBuffer : public AuxiliaryDataHolder {
public:
	explicit JSONStringVectorBuffer(shared_ptr<JSONAllocator> allocator_p) : allocator(std::move(allocator_p)) {
	}

private:
	shared_ptr<JSONAllocator> allocator;
};

//! JSON allocator is a custom allocator for yyjson that prevents many tiny allocations
class JSONAllocator : public enable_shared_from_this<JSONAllocator> {
public:
	explicit JSONAllocator(Allocator &allocator)
	    : arena_allocator(allocator), yyjson_allocator({Allocate, Reallocate, Free, this}) {
	}

	inline yyjson_alc *GetYYAlc() {
		return &yyjson_allocator;
	}

	void Reset() {
		arena_allocator.Reset();
	}

	void AddBuffer(Vector &vector) {
		if (vector.GetType().InternalType() == PhysicalType::VARCHAR) {
			StringVector::AddAuxiliaryData(vector, make_uniq<JSONStringVectorBuffer>(shared_from_this()));
		}
	}

	static void AddBuffer(Vector &vector, yyjson_alc *alc) {
		auto alloc = (JSONAllocator *)alc->ctx; // NOLINT
		alloc->AddBuffer(vector);
	}

private:
	static inline void *Allocate(void *ctx, size_t size) {
		auto alloc = (JSONAllocator *)ctx; // NOLINT
		return alloc->arena_allocator.AllocateAligned(size);
	}

	static inline void *Reallocate(void *ctx, void *ptr, size_t old_size, size_t size) {
		auto alloc = (JSONAllocator *)ctx; // NOLINT
		return alloc->arena_allocator.ReallocateAligned(data_ptr_cast(ptr), old_size, size);
	}

	static inline void Free(void *ctx, void *ptr) {
		// NOP because ArenaAllocator can't free
	}

private:
	ArenaAllocator arena_allocator;
	yyjson_alc yyjson_allocator;
};

class ConvertedJSONHolder {
public:
	explicit ConvertedJSONHolder(Allocator &allocator) : allocator(JSONAllocator(allocator)) {
	}
	~ConvertedJSONHolder() {
		if (doc) {
			yyjson_mut_doc_free(doc);
		}
	}

public:
	optional<string_t> Serialize(const yyjson_mut_val *value, string &error) {
		yyjson_write_err write_error;
		size_t size;

		const auto *serialized =
		    yyjson_mut_val_write_opts(value, YYJSON_WRITE_ALLOW_INF_AND_NAN, GetAllocator(), &size, &write_error);
		if (!serialized) {
			error = StringUtil::Format("Failed to serialize JSON: %s", write_error.msg);
			return {};
		}

		return string_t(serialized, NumericCast<uint32_t>(size));
	}
	yyjson_mut_doc *GetDocument() {
		if (!doc) {
			doc = yyjson_mut_doc_new(allocator.GetYYAlc());
			if (!doc) {
				throw OutOfMemoryException("Failed to create yyjson document");
			}
		}

		return doc;
	}
	void Reset() {
		if (doc) {
			yyjson_mut_doc_free(doc);
			doc = nullptr;
		}
		allocator.Reset();
	}

private:
	yyjson_alc *GetAllocator() {
		return allocator.GetYYAlc();
	}

private:
	JSONAllocator allocator;
	yyjson_mut_doc *doc = nullptr;
};

} // namespace duckdb
