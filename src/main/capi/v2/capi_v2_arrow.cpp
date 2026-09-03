#include "duckdb/main/capi_v2/capi_v2_result_internal.hpp"

#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_util.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/arrow/nanoarrow/nanoarrow.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/main/chunk_scan_state.hpp"

#include <cerrno>

namespace duckdb::capiv2 {

//! The converter is the engine's resolved Arrow table schema, handed out directly.
using CV2ArrowConverter = duckdb::ArrowTableSchema;

auto Convert(duckdb_v2_arrow_converter_handle converter) -> CV2ArrowConverter * {
	return reinterpret_cast<CV2ArrowConverter *>(converter);
}
auto Convert(CV2ArrowConverter *converter) -> duckdb_v2_arrow_converter_handle {
	return reinterpret_cast<duckdb_v2_arrow_converter_handle>(converter);
}

namespace {

//! What batch_size 0 selects: pyarrow's dataset-scanner default, and 64 vectors in a default
//! build, so one Arrow array coalesces a clean 64 engine chunks.
constexpr idx_t CV2_DEFAULT_ARROW_BATCH_SIZE = 131072;

//----------------------------------------------------------------------------------------------------------------------
// result_to_arrow_stream
//----------------------------------------------------------------------------------------------------------------------

//! Drives a V2 result through the engine's chunk-cursor interface, so ArrowUtil::TryFetchChunk
//! (offset tracking plus appender coalescing) can pull from it. End of stream is signalled the
//! way QueryResultChunkScanState signals it: a null current chunk.
class CV2ArrowScanState : public ChunkScanState {
public:
	explicit CV2ArrowScanState(ResultWrapperV2 &wrapper) : wrapper(wrapper) {
	}

	bool LoadNextChunk(ErrorData &error) override {
		if (finished) {
			current_chunk = nullptr;
			return true;
		}
		try {
			current_chunk = wrapper.FetchChunkBlocking();
		} catch (std::exception &ex) {
			scan_error = ErrorData(ex);
			has_scan_error = true;
			finished = true;
			current_chunk = nullptr;
			error = scan_error;
			return false;
		}
		offset = 0;
		if (!current_chunk) {
			finished = true;
		}
		return true;
	}
	bool HasError() const override {
		return has_scan_error;
	}
	ErrorData &GetError() override {
		return scan_error;
	}
	const vector<LogicalType> &Types() const override {
		return wrapper.types;
	}
	const vector<Identifier> &Names() const override {
		return wrapper.names;
	}

private:
	ResultWrapperV2 &wrapper;
	ErrorData scan_error;
	bool has_scan_error = false;
};

//! The stream's private_data. Owns the result state machine -- and through it the query's
//! transaction and the connection's live-result slot -- the cursor driving it, and the schema
//! cached while the producing transaction was still live.
struct CV2ArrowStream {
	unique_ptr<ResultWrapperV2> wrapper;
	unique_ptr<ChunkScanState> scan_state;
	ArrowSchema cached_schema {};
	unordered_map<idx_t, const shared_ptr<ArrowTypeExtensionData>> extension_types;
	ClientProperties client_properties;
	idx_t batch_size = CV2_DEFAULT_ARROW_BATCH_SIZE;
	ErrorData last_error;

	~CV2ArrowStream() {
		if (cached_schema.release) {
			cached_schema.release(&cached_schema);
		}
		// The scan state holds a reference into *wrapper, so drop it first.
		scan_state.reset();
	}
};

//! An Arrow callback must not let an exception cross the C ABI, so each one is wrapped whole and
//! reports through the errno-style return code the interface specifies.
int CV2ArrowStreamGetSchema(ArrowArrayStream *stream, ArrowSchema *out) {
	if (!stream->release || !stream->private_data) {
		return EINVAL;
	}
	auto &self = *static_cast<CV2ArrowStream *>(stream->private_data);
	try {
		if (!self.cached_schema.release) {
			self.last_error = ErrorData("arrow stream: the schema is unavailable");
			return EINVAL;
		}
		// The consumer owns what get_schema returns and releases it independently of the
		// stream, so hand out a deep copy. Copying the cached schema is pure: it never
		// re-reads the catalog, which is the point of having cached it.
		if (duckdb_nanoarrow::ArrowSchemaDeepCopy(&self.cached_schema, out) != NANOARROW_OK) {
			self.last_error = ErrorData("arrow stream: failed to copy the schema");
			return ENOMEM;
		}
		return 0;
	} catch (std::exception &ex) {
		// Recording the message is itself best-effort under memory pressure; the return code
		// is what the consumer must rely on.
		try {
			self.last_error = ErrorData(ex);
		} catch (...) { // NOLINT: best-effort
		}
		return EIO;
	} catch (...) {
		return EIO;
	}
}

int CV2ArrowStreamGetNext(ArrowArrayStream *stream, ArrowArray *out) {
	if (!stream->release || !stream->private_data) {
		return EINVAL;
	}
	auto &self = *static_cast<CV2ArrowStream *>(stream->private_data);
	out->release = nullptr;
	try {
		idx_t result_count = 0;
		ErrorData error;
		if (!ArrowUtil::TryFetchChunk(*self.scan_state, self.client_properties, self.batch_size, out, result_count,
		                              error, self.extension_types)) {
			self.last_error = error;
			return EIO;
		}
		if (result_count == 0) {
			// End of stream, which the interface spells as a released array.
			out->release = nullptr;
		}
	} catch (std::exception &ex) {
		self.last_error = ErrorData(ex);
		return EIO;
	} catch (...) {
		return EIO;
	}
	return 0;
}

const char *CV2ArrowStreamGetLastError(ArrowArrayStream *stream) {
	if (!stream->release || !stream->private_data) {
		return "arrow stream was released";
	}
	auto &self = *static_cast<CV2ArrowStream *>(stream->private_data);
	return self.last_error.Message().c_str();
}

void CV2ArrowStreamRelease(ArrowArrayStream *stream) {
	if (!stream || !stream->release) {
		return;
	}
	stream->release = nullptr;
	auto self = static_cast<CV2ArrowStream *>(stream->private_data);
	stream->private_data = nullptr;
	if (!self) {
		return;
	}
	// Mirror result_destroy: close the engine result and roll back any transaction the bridge
	// injected, before freeing. A release callback must not throw across the C ABI.
	self->scan_state.reset();
	try {
		if (self->wrapper) {
			self->wrapper->Finalize();
		}
	} catch (...) { // NOLINT: best-effort cleanup
	}
	// Frees the cached schema, and the wrapper, whose destructor releases the busy slot.
	delete self;
}

} // namespace
} // namespace duckdb::capiv2

//----------------------------------------------------------------------------------------------------------------------
// Public API
//----------------------------------------------------------------------------------------------------------------------

using namespace duckdb::capiv2;

DUCKDB_V2_ERROR duckdb_v2_result_to_arrow_stream(duckdb_v2_result_handle *result, idx_t batch_size,
                                                 struct ArrowArrayStream *out_stream,
                                                 duckdb_v2_error_info_handle *err) {
	// Validate before taking ownership, so a rejection leaves the caller's result intact.
	DUCKDB_CHECK_ARG(result);
	DUCKDB_CHECK_ARG(*result);
	DUCKDB_CHECK_ARG(out_stream);
	return WithErrorHandler(err, [&]() {
		// Adopt by transfer; consumed on success and failure alike.
		auto wrapper = duckdb::unique_ptr<ResultWrapperV2>(Convert(*result));
		*result = nullptr;
		try {
			// The schema must be built while the query's transaction is live, so advance to the
			// principal fragment if its metadata is not available yet. No rows can be produced
			// before that fragment is prepared, so stepping here never drops data.
			while (!wrapper->metadata_available) {
				duckdb::unique_ptr<duckdb::DataChunk> discard;
				auto status = wrapper->Step(discard);
				if (status == DUCKDB_V2_RESULT_STEP_STATUS_WAITING) {
					wrapper->Wait();
					continue;
				}
				if (status == DUCKDB_V2_RESULT_STEP_STATUS_CHUNK) {
					throw duckdb::InternalException(
					    "arrow stream: a row was produced before result metadata was available");
				}
				break; // FINISHED / CANCELLED: no row-producing fragment.
			}
			if (!wrapper->context) {
				throw duckdb::InvalidInputException("result is not associated with an active context");
			}
			auto &context = *wrapper->context;

			auto self = duckdb::make_uniq<CV2ArrowStream>();
			self->batch_size = batch_size == 0 ? CV2_DEFAULT_ARROW_BATCH_SIZE : batch_size;
			self->client_properties = context.GetClientProperties();
			// Cache the schema and the extension type map now, under the live transaction:
			// populate-schema callbacks and ENUM dictionaries read the catalog, which get_schema
			// cannot do once the transaction is gone.
			self->extension_types = duckdb::ArrowTypeExtensionData::GetExtensionTypes(context, wrapper->types);
			duckdb::ArrowConverter::ToArrowSchema(&self->cached_schema, wrapper->types,
			                                      duckdb::IdentifiersToStrings(wrapper->names),
			                                      self->client_properties);
			self->scan_state = duckdb::make_uniq<CV2ArrowScanState>(*wrapper);
			self->wrapper = std::move(wrapper);

			out_stream->get_schema = CV2ArrowStreamGetSchema;
			out_stream->get_next = CV2ArrowStreamGetNext;
			out_stream->get_last_error = CV2ArrowStreamGetLastError;
			out_stream->release = CV2ArrowStreamRelease;
			out_stream->private_data = self.release();
		} catch (...) {
			// A throw before ownership moved into the stream leaves the result with us. Finalize
			// it so a failed export cleans up exactly as result_destroy would, rather than
			// leaving the query open until the local pointer goes out of scope.
			if (wrapper) {
				try {
					wrapper->Finalize();
				} catch (...) { // NOLINT: never mask the original error
				}
			}
			throw;
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_logical_types_to_arrow_schema(duckdb_v2_context_handle context,
                                                        const duckdb_v2_logical_type_handle *types,
                                                        const duckdb_v2_str *names, idx_t count,
                                                        struct ArrowSchema *out_schema,
                                                        duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(context);
	DUCKDB_CHECK_ARG(out_schema);
	if (count > 0 && !types) {
		return NullArgumentError(err, __func__, "types");
	}
	if (count > 0 && !names) {
		return NullArgumentError(err, __func__, "names");
	}
	return WithErrorHandler(err, [&]() {
		duckdb::vector<duckdb::LogicalType> schema_types;
		duckdb::vector<duckdb::string> schema_names;
		schema_types.reserve(count);
		schema_names.reserve(count);
		for (idx_t i = 0; i < count; i++) {
			if (!types[i]) {
				throw duckdb::InvalidInputException("null logical type at index %llu", i);
			}
			if (IsNullArgument(names[i])) {
				throw duckdb::InvalidInputException("malformed column name at index %llu", i);
			}
			schema_types.push_back(*Convert(types[i]));
			schema_names.emplace_back(Convert(names[i]));
		}
		auto properties = Convert(context)->GetClientProperties();
		duckdb::ArrowConverter::ToArrowSchema(out_schema, schema_types, schema_names, properties);
	});
}

DUCKDB_V2_ERROR duckdb_v2_data_chunk_to_arrow_array(duckdb_v2_context_handle context, duckdb_v2_data_chunk_handle chunk,
                                                    struct ArrowArray *out_array, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(context);
	DUCKDB_CHECK_ARG(chunk);
	DUCKDB_CHECK_ARG(out_array);
	return WithErrorHandler(err, [&]() {
		auto &ctx = *Convert(context);
		auto &data_chunk = *Convert(chunk);
		auto properties = ctx.GetClientProperties();
		auto extension_types = duckdb::ArrowTypeExtensionData::GetExtensionTypes(ctx, data_chunk.GetTypes());
		duckdb::ArrowConverter::ToArrowArray(data_chunk, out_array, properties, extension_types);
	});
}

DUCKDB_V2_ERROR duckdb_v2_arrow_converter_create(duckdb_v2_context_handle context, struct ArrowSchema *schema,
                                                 duckdb_v2_arrow_converter_handle *out_converter,
                                                 duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(context);
	DUCKDB_CHECK_ARG(schema);
	DUCKDB_CHECK_ARG(out_converter);
	*out_converter = nullptr;
	return WithErrorHandler(err, [&]() {
		auto converter = duckdb::make_uniq<CV2ArrowConverter>();
		duckdb::ArrowTableFunction::PopulateArrowTableSchema(*Convert(context), *converter, *schema);
		*out_converter = Convert(converter.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_arrow_converter_array_to_chunk(duckdb_v2_context_handle context, struct ArrowArray *array,
                                                         duckdb_v2_arrow_converter_handle converter,
                                                         duckdb_v2_data_chunk_handle *out_chunk,
                                                         duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(context);
	DUCKDB_CHECK_ARG(array);
	DUCKDB_CHECK_ARG(converter);
	DUCKDB_CHECK_ARG(out_chunk);
	*out_chunk = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &ctx = *Convert(context);
		auto &resolved = *Convert(converter);
		auto &types = resolved.GetTypes();
		auto &arrow_types = resolved.GetColumns();

		auto chunk = duckdb::make_uniq<duckdb::DataChunk>();
		chunk->Initialize(duckdb::Allocator::DefaultAllocator(), types, duckdb::NumericCast<idx_t>(array->length));
		// Check the array against the converter before indexing into its children, which would
		// otherwise run off the end.
		if (duckdb::NumericCast<idx_t>(array->n_children) != chunk->ColumnCount()) {
			throw duckdb::InvalidInputException("Arrow array child count does not match the converter column count");
		}
		chunk->SetChildCardinality(duckdb::NumericCast<idx_t>(array->length));

		// One shared owner for the whole foreign array, transferred once. The chunk's zero-copy
		// vectors keep it alive, and a column that copies instead still leaves it owned here.
		// This mirrors the engine's scan path: a per-column wrapper would free the shared parent
		// after the first copying column and dangle the rest.
		auto owned_array = duckdb::make_shared_ptr<duckdb::ArrowArrayWrapper>();
		owned_array->arrow_array = *array;
		array->release = nullptr;
		auto &parent_array = owned_array->arrow_array;
		if (chunk->ColumnCount() > 0 && !parent_array.children) {
			throw duckdb::InvalidInputException("Arrow array has null children");
		}
		for (duckdb::idx_t i = 0; i < chunk->ColumnCount(); i++) {
			auto *child_array = parent_array.children[i];
			// Validate each foreign child before handing it to the engine, so a malformed array
			// is an input error rather than a crash inside the conversion.
			if (!child_array || !child_array->release) {
				throw duckdb::InvalidInputException("Arrow array child is null or already released");
			}
			if (child_array->length != parent_array.length) {
				throw duckdb::InvalidInputException("Arrow array child length does not match the array length");
			}
			auto arrow_type = arrow_types.at(i);
			auto array_state = duckdb::make_uniq<duckdb::ArrowArrayScanState>(ctx);
			array_state->owned_data = owned_array;
			switch (arrow_type->GetPhysicalType()) {
			case duckdb::ArrowArrayPhysicalType::DICTIONARY_ENCODED:
				if (!child_array->dictionary) {
					throw duckdb::InvalidInputException("Dictionary-encoded Arrow array has no dictionary");
				}
				duckdb::ArrowToDuckDBConversion::ColumnArrowToDuckDBDictionary(
				    chunk->data[i], *child_array, 0, *array_state, chunk->size(), *arrow_type);
				break;
			case duckdb::ArrowArrayPhysicalType::RUN_END_ENCODED:
				duckdb::ArrowToDuckDBConversion::ColumnArrowToDuckDBRunEndEncoded(
				    chunk->data[i], *child_array, 0, *array_state, chunk->size(), *arrow_type);
				break;
			case duckdb::ArrowArrayPhysicalType::DEFAULT:
				duckdb::ArrowToDuckDBConversion::SetValidityMask(chunk->data[i], *child_array, 0, chunk->size(),
				                                                 parent_array.offset, -1);
				duckdb::ArrowToDuckDBConversion::ColumnArrowToDuckDB(chunk->data[i], *child_array, 0, *array_state,
				                                                     chunk->size(), *arrow_type);
				break;
			default:
				throw duckdb::NotImplementedException("Only default Arrow physical types are currently supported");
			}
			// Parity with the engine's own scan loop, which re-asserts the size after each
			// column: a dictionary or run-end conversion replaces the vector rather than filling
			// it. It is inert here, because the chunk is freshly allocated per call and every
			// vector is sized before the conversion, but it becomes load-bearing the moment a
			// chunk is reused across conversions.
			duckdb::FlatVector::SetSize(chunk->data[i], duckdb::count_t(chunk->size()));
		}
		*out_chunk = Convert(chunk.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_arrow_converter_get_schema(duckdb_v2_arrow_converter_handle converter,
                                                     duckdb_v2_schema_handle *out_schema,
                                                     duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(converter);
	DUCKDB_CHECK_ARG(out_schema);
	*out_schema = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &resolved = *Convert(converter);
		auto &types = resolved.GetTypes();
		auto &names = resolved.GetNames();
		D_ASSERT(names.size() == types.size());
		// The same owned schema shape statement_bind produces; the caller destroys it.
		auto schema = duckdb::make_uniq<CV2Schema>();
		for (duckdb::idx_t i = 0; i < types.size(); i++) {
			schema->fields.push_back({names[i], types[i]});
		}
		*out_schema = Convert(schema.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_arrow_converter_destroy(duckdb_v2_arrow_converter_handle *converter) {
	return WithErrorHandler(nullptr, [&]() {
		if (!converter) {
			return;
		}
		if (*converter) {
			delete Convert(*converter);
			*converter = nullptr;
		}
	});
}
