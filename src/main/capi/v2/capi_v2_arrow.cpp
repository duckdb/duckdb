#include "duckdb/main/capi_v2/capi_v2_result_internal.hpp"

#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_util.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/arrow/nanoarrow/nanoarrow.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/main/chunk_scan_state.hpp"

#include <cerrno>
#include <deque>

namespace duckdb::capiv2 {
namespace {

//! What batch_size 0 selects for a result stream: 64 vectors in a default build, so one Arrow array gathers a clean 64
//! engine chunks.
constexpr idx_t CV2_DEFAULT_ARROW_BATCH_SIZE = 131072;

} // namespace

//----------------------------------------------------------------------------------------------------------------------
// arrow_importer
//----------------------------------------------------------------------------------------------------------------------

//! Reads Arrow arrays into data chunks against one resolved schema. Holds at most one array in flight and slices it
//! lazily, so a long array does not have to be materialized all at once.
class CV2ArrowImporter {
public:
	//! The resolved correspondence: DuckDB types plus the per-column Arrow type information.
	ArrowTableSchema table;
	//! Borrowed from creation: every conversion runs under it, so the importer must not outlive it. Within a connection
	//! this is one context throughout, so a bind-time importer works from the matching exec callback.
	optional_ptr<ClientContext> context;
	//! The most rows a produced chunk may hold; 0 means no maximum.
	idx_t batch_size = 0;

	//! Set when the array was handed over: the chunks reference its buffers zero-copy and keep this alive themselves,
	//! so they may outlive the importer.
	shared_ptr<ArrowArrayWrapper> owned_array;
	//! Set when the caller kept the array: borrowed for the drain, and every produced chunk is materialized so it does
	//! not reference it.
	optional_ptr<ArrowArray> borrowed_array;
	//! Rows of the in-flight array already handed out.
	idx_t offset = 0;
	idx_t length = 0;
	//! Set by an append asking to flush: the rows left over when the array runs out come out as a short chunk rather
	//! than waiting for a batch that will never fill.
	bool flush_on_drain = false;

	//! Rows held back from a previous array because they did not fill a batch. Materialized, since a chunk that spans
	//! two arrays cannot reference either of them. Always fewer than batch_size rows.
	unique_ptr<DataChunk> pending;

	auto HasInputRows() const -> bool {
		return offset < length;
	}
	auto PendingRows() const -> idx_t {
		return pending ? pending->size() : 0;
	}
	//! The in-flight array, whichever way it arrived.
	auto Array() -> ArrowArray & {
		return owned_array ? owned_array->arrow_array : *borrowed_array;
	}
	void ReleaseInput() {
		owned_array.reset();
		borrowed_array = nullptr;
		offset = 0;
		length = 0;
	}
};

auto Convert(duckdb_v2_arrow_importer_handle importer) -> CV2ArrowImporter * {
	return reinterpret_cast<CV2ArrowImporter *>(importer);
}
auto Convert(CV2ArrowImporter *importer) -> duckdb_v2_arrow_importer_handle {
	return reinterpret_cast<duckdb_v2_arrow_importer_handle>(importer);
}

//----------------------------------------------------------------------------------------------------------------------
// arrow_exporter
//----------------------------------------------------------------------------------------------------------------------

//! Writes data chunks out as Arrow arrays for one fixed column list. The mirror of the importer: chunks go in, and
//! arrays of at most batch_size rows come out, gathering across chunks when one is too short to fill a batch. The Arrow
//! settings are pinned at construction so the schema and the arrays cannot disagree.
class CV2ArrowExporter {
public:
	vector<LogicalType> types;
	vector<string> names;
	//! Pinned at construction: the schema this exporter reports is built from these, and so is every array, which is
	//! what keeps the two consistent.
	ClientProperties properties;
	unordered_map<idx_t, const shared_ptr<ArrowTypeExtensionData>> extension_types;
	//! The most rows an array may hold; 0 means no maximum.
	idx_t batch_size = 0;

	//! Live while rows have been gathered but not yet finalized into an array.
	unique_ptr<ArrowAppender> appender;
	idx_t gathered = 0;
	//! Completed arrays waiting to be taken.
	std::deque<ArrowArray> ready;

	~CV2ArrowExporter() {
		for (auto &array : ready) {
			if (array.release) {
				array.release(&array);
			}
		}
	}

	void EnsureAppender(idx_t capacity) {
		if (!appender) {
			appender = make_uniq<ArrowAppender>(types, capacity, properties, extension_types);
			gathered = 0;
		}
	}

	//! Finalizes the gathered rows into a queued array. A no-op with nothing gathered.
	void Emit() {
		if (appender && gathered > 0) {
			ready.push_back(appender->Finalize());
		}
		appender.reset();
		gathered = 0;
	}
};

auto Convert(duckdb_v2_arrow_exporter_handle exporter) -> CV2ArrowExporter * {
	return reinterpret_cast<CV2ArrowExporter *>(exporter);
}
auto Convert(CV2ArrowExporter *exporter) -> duckdb_v2_arrow_exporter_handle {
	return reinterpret_cast<duckdb_v2_arrow_exporter_handle>(exporter);
}

namespace {

//----------------------------------------------------------------------------------------------------------------------
// result_to_arrow_stream
//----------------------------------------------------------------------------------------------------------------------

//! Drives a V2 result through the engine's chunk-cursor interface, so ArrowUtil::TryFetchChunk (offset tracking plus
//! appender coalescing) can pull from it. End of stream is signalled the way QueryResultChunkScanState signals it: a
//! null current chunk.
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

//! The stream's private_data. Owns the result state machine -- and through it the query's transaction and the
//! connection's live-result slot -- the cursor driving it, and the schema cached while the producing transaction was
//! still live.
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

//! An Arrow callback must not let an exception cross the C ABI, so each one is wrapped whole and reports through the
//! errno-style return code the interface specifies.
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
		// The consumer owns what get_schema returns and releases it independently of the stream, so hand out a deep
		// copy. Copying the cached schema is pure: it never re-reads the catalog, which is the point of having cached
		// it.
		if (duckdb_nanoarrow::ArrowSchemaDeepCopy(&self.cached_schema, out) != NANOARROW_OK) {
			self.last_error = ErrorData("arrow stream: failed to copy the schema");
			return ENOMEM;
		}
		return 0;
	} catch (std::exception &ex) {
		// Recording the message is itself best-effort under memory pressure; the return code is what the consumer must
		// rely on.
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
	// Mirror result_destroy: close the engine result and roll back any transaction the bridge injected, before freeing.
	// A release callback must not throw across the C ABI.
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

//----------------------------------------------------------------------------------------------------------------------
// Import conversion
//----------------------------------------------------------------------------------------------------------------------

//! Converts `rows` rows starting at `from` of `array` into a fresh chunk, through the resolved per-column Arrow types.
//! `owner` is the shared owner the chunk's zero-copy vectors keep alive; it is null when the caller kept the array, in
//! which case the result is materialized instead.
auto CV2ConvertArrowSlice(ClientContext &context, CV2ArrowImporter &importer, ArrowArray &array,
                          const shared_ptr<ArrowArrayWrapper> &owner, idx_t from, idx_t rows) -> unique_ptr<DataChunk> {
	auto &types = importer.table.GetTypes();
	auto &arrow_types = importer.table.GetColumns();

	auto chunk = make_uniq<DataChunk>();
	chunk->Initialize(Allocator::DefaultAllocator(), types, MaxValue<idx_t>(rows, 1));
	chunk->SetChildCardinality(rows);
	for (idx_t i = 0; i < chunk->ColumnCount(); i++) {
		auto *child_array = array.children[i];
		auto arrow_type = arrow_types.at(i);
		// A fresh scan state per slice, so nothing cached for one array leaks into the next. The cost is re-decoding a
		// dictionary per chunk.
		auto array_state = make_uniq<ArrowArrayScanState>(context);
		array_state->owned_data = owner;
		switch (arrow_type->GetPhysicalType()) {
		case ArrowArrayPhysicalType::DICTIONARY_ENCODED:
			if (!child_array->dictionary) {
				throw InvalidInputException("Dictionary-encoded Arrow array has no dictionary");
			}
			ArrowToDuckDBConversion::ColumnArrowToDuckDBDictionary(chunk->data[i], *child_array, from, *array_state,
			                                                       rows, *arrow_type);
			break;
		case ArrowArrayPhysicalType::RUN_END_ENCODED:
			ArrowToDuckDBConversion::ColumnArrowToDuckDBRunEndEncoded(chunk->data[i], *child_array, from, *array_state,
			                                                          rows, *arrow_type);
			break;
		case ArrowArrayPhysicalType::DEFAULT:
			ArrowToDuckDBConversion::SetValidityMask(chunk->data[i], *child_array, from, rows, array.offset, -1);
			ArrowToDuckDBConversion::ColumnArrowToDuckDB(chunk->data[i], *child_array, from, *array_state, rows,
			                                             *arrow_type);
			break;
		default:
			throw NotImplementedException("Only default Arrow physical types are currently supported");
		}
		// Re-assert the size after the conversion, mirroring the engine's own scan loop: a dictionary or run-end
		// conversion replaces the vector rather than filling it.
		FlatVector::SetSize(chunk->data[i], count_t(rows));
	}
	chunk->CheckCardinality(rows);
	if (owner) {
		return chunk;
	}
	// The caller kept the array, so the chunk must not reference it: copy the rows out into flat vectors that own their
	// memory.
	auto materialized = make_uniq<DataChunk>();
	materialized->Initialize(Allocator::DefaultAllocator(), types, MaxValue<idx_t>(rows, 1));
	chunk->Copy(*materialized, 0);
	return materialized;
}

//! Rejects an array whose shape disagrees with the resolved schema before anything reads it, so a malformed array is an
//! input error rather than a crash inside the conversion.
void CV2ValidateArrowArray(CV2ArrowImporter &importer, ArrowArray &array) {
	auto column_count = importer.table.GetTypes().size();
	if (NumericCast<idx_t>(array.n_children) != column_count) {
		throw InvalidInputException("Arrow array child count does not match the importer column count");
	}
	if (column_count > 0 && !array.children) {
		throw InvalidInputException("Arrow array has null children");
	}
	for (idx_t i = 0; i < column_count; i++) {
		auto *child_array = array.children[i];
		if (!child_array || !child_array->release) {
			throw InvalidInputException("Arrow array child is null or already released");
		}
		if (child_array->length != array.length) {
			throw InvalidInputException("Arrow array child length does not match the array length");
		}
	}
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
			// The schema must be built while the query's transaction is live, so advance to the principal fragment if
			// its metadata is not available yet. No rows can be produced before that fragment is prepared, so stepping
			// here never drops data.
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
			// Cache the schema and the extension type map now, under the live transaction: populate-schema callbacks
			// and ENUM dictionaries read the catalog, which get_schema cannot do once the transaction is gone.
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
			// A throw before ownership moved into the stream leaves the result with us. Finalize it so a failed export
			// cleans up exactly as result_destroy would, rather than leaving the query open until the local pointer
			// goes out of scope.
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

//----------------------------------------------------------------------------------------------------------------------
// arrow_importer
//----------------------------------------------------------------------------------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_arrow_importer_create(duckdb_v2_context_handle context, struct ArrowSchema *schema,
                                                idx_t batch_size, duckdb_v2_arrow_importer_handle *out_importer,
                                                duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(context);
	DUCKDB_CHECK_ARG(schema);
	DUCKDB_CHECK_ARG(out_importer);
	*out_importer = nullptr;
	return WithErrorHandler(err, [&]() {
		auto importer = duckdb::make_uniq<CV2ArrowImporter>();
		importer->context = Convert(context);
		importer->batch_size = batch_size;
		duckdb::ArrowTableFunction::PopulateArrowTableSchema(*Convert(context), importer->table, *schema);
		*out_importer = Convert(importer.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_arrow_importer_get_schema(duckdb_v2_arrow_importer_handle importer,
                                                    duckdb_v2_schema_handle *out_schema,
                                                    duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(importer);
	DUCKDB_CHECK_ARG(out_schema);
	*out_schema = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &table = Convert(importer)->table;
		auto &types = table.GetTypes();
		auto &names = table.GetNames();
		D_ASSERT(names.size() == types.size());
		// The same owned schema shape statement_bind produces; the caller destroys it.
		auto schema = duckdb::make_uniq<CV2Schema>();
		for (duckdb::idx_t i = 0; i < types.size(); i++) {
			schema->fields.push_back({names[i], types[i]});
		}
		*out_schema = Convert(schema.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_arrow_importer_append(duckdb_v2_arrow_importer_handle importer, struct ArrowArray *array,
                                                bool consume, bool flush, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(importer);
	return WithErrorHandler(err, [&]() {
		auto &self = *Convert(importer);
		if (self.HasInputRows()) {
			throw duckdb::InvalidInputException(
			    "the previous Arrow array still has rows; drain it with arrow_importer_next_chunk first");
		}
		if (!array) {
			// A missing array is allowed only to ask for a flush, which releases the held rows as a short chunk.
			if (!flush) {
				throw duckdb::InvalidInputException("null argument 'array' to duckdb_v2_arrow_importer_append");
			}
			self.flush_on_drain = true;
			return;
		}
		// Validate before taking anything, so a rejected array is left untouched.
		CV2ValidateArrowArray(self, *array);
		self.ReleaseInput();
		if (consume) {
			// Adopt the array; the chunks reference its buffers and keep this wrapper alive.
			auto owned = duckdb::make_shared_ptr<duckdb::ArrowArrayWrapper>();
			owned->arrow_array = *array;
			array->release = nullptr;
			self.owned_array = std::move(owned);
		} else {
			self.borrowed_array = array;
		}
		self.length = duckdb::NumericCast<duckdb::idx_t>(self.Array().length);
		self.offset = 0;
		self.flush_on_drain = flush;
	});
}

DUCKDB_V2_ERROR duckdb_v2_arrow_importer_next_chunk(duckdb_v2_arrow_importer_handle importer,
                                                    duckdb_v2_data_chunk_handle *out_chunk,
                                                    duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(importer);
	DUCKDB_CHECK_ARG(out_chunk);
	*out_chunk = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &self = *Convert(importer);
		auto remaining = self.HasInputRows() ? self.length - self.offset : 0;
		auto pending_rows = self.PendingRows();
		if (remaining == 0 && pending_rows == 0) {
			self.ReleaseInput();
			return; // drained, or nothing appended
		}
		// Rows carried over from a previous array have to be joined with the head of this one, which is the one case an
		// imported chunk cannot reference its array.
		if (pending_rows > 0) {
			auto need = self.batch_size == 0 ? remaining : self.batch_size - pending_rows;
			auto take = duckdb::MinValue<duckdb::idx_t>(need, remaining);
			if (take > 0) {
				auto slice = CV2ConvertArrowSlice(*self.context, self, self.Array(), nullptr, self.offset, take);
				self.pending->Append(*slice, duckdb::VectorAppendMode::ALLOW_RESIZE);
				self.offset += take;
				pending_rows += take;
			}
			auto batch_full = self.batch_size != 0 && pending_rows >= self.batch_size;
			if (!batch_full && !self.flush_on_drain) {
				return; // hold the partial batch until more input arrives
			}
			*out_chunk = Convert(self.pending.release());
			return;
		}
		auto take = self.batch_size == 0 ? remaining : duckdb::MinValue<duckdb::idx_t>(self.batch_size, remaining);
		// A tail too short to fill a batch is carried over, unless the caller asked to flush. Everything else converts
		// straight out of the array, zero-copy when it was handed over.
		auto is_short = self.batch_size != 0 && take < self.batch_size;
		auto owner = self.owned_array;
		if (is_short && !self.flush_on_drain) {
			self.pending = CV2ConvertArrowSlice(*self.context, self, self.Array(), nullptr, self.offset, take);
			self.offset += take;
			return;
		}
		auto chunk = CV2ConvertArrowSlice(*self.context, self, self.Array(), owner, self.offset, take);
		self.offset += take;
		*out_chunk = Convert(chunk.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_arrow_importer_destroy(duckdb_v2_arrow_importer_handle *importer) {
	return WithErrorHandler(nullptr, [&]() {
		if (!importer) {
			return;
		}
		if (*importer) {
			delete Convert(*importer);
			*importer = nullptr;
		}
	});
}

//----------------------------------------------------------------------------------------------------------------------
// arrow_exporter
//----------------------------------------------------------------------------------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_arrow_exporter_create(duckdb_v2_context_handle context,
                                                const duckdb_v2_logical_type_handle *types, const duckdb_v2_str *names,
                                                idx_t count, idx_t batch_size,
                                                duckdb_v2_arrow_exporter_handle *out_exporter,
                                                duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(context);
	DUCKDB_CHECK_ARG(out_exporter);
	*out_exporter = nullptr;
	if (count > 0 && !types) {
		return NullArgumentError(err, __func__, "types");
	}
	if (count > 0 && !names) {
		return NullArgumentError(err, __func__, "names");
	}
	return WithErrorHandler(err, [&]() {
		auto &ctx = *Convert(context);
		auto exporter = duckdb::make_uniq<CV2ArrowExporter>();
		exporter->batch_size = batch_size;
		exporter->types.reserve(count);
		exporter->names.reserve(count);
		for (idx_t i = 0; i < count; i++) {
			if (!types[i]) {
				throw duckdb::InvalidInputException("null logical type at index %llu", i);
			}
			if (IsNullArgument(names[i])) {
				throw duckdb::InvalidInputException("malformed column name at index %llu", i);
			}
			exporter->types.push_back(*Convert(types[i]));
			exporter->names.emplace_back(Convert(names[i]));
		}
		// Pin the settings and the extension map now: the schema below and every array this exporter produces are built
		// from them, which is what keeps the two consistent.
		exporter->properties = ctx.GetClientProperties();
		exporter->extension_types = duckdb::ArrowTypeExtensionData::GetExtensionTypes(ctx, exporter->types);
		*out_exporter = Convert(exporter.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_arrow_exporter_get_schema(duckdb_v2_arrow_exporter_handle exporter,
                                                    struct ArrowSchema *out_schema, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(exporter);
	DUCKDB_CHECK_ARG(out_schema);
	return WithErrorHandler(err, [&]() {
		auto &self = *Convert(exporter);
		duckdb::ArrowConverter::ToArrowSchema(out_schema, self.types, self.names, self.properties);
	});
}

DUCKDB_V2_ERROR duckdb_v2_arrow_exporter_append(duckdb_v2_arrow_exporter_handle exporter,
                                                duckdb_v2_data_chunk_handle *chunk, bool consume, bool flush,
                                                duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(exporter);
	return WithErrorHandler(err, [&]() {
		auto &self = *Convert(exporter);
		if (chunk && *chunk) {
			auto &input = *Convert(*chunk);
			if (input.GetTypes() != self.types) {
				throw duckdb::InvalidInputException("chunk types do not match the types the exporter was created with");
			}
			// The chunk is read in full before this returns -- the conversion copies into freshly allocated Arrow
			// buffers -- filling batches of at most batch_size rows and carrying whatever is left over into the next
			// append.
			auto rows = input.size();
			for (duckdb::idx_t consumed = 0; consumed < rows;) {
				self.EnsureAppender(self.batch_size == 0 ? rows : self.batch_size);
				auto room = self.batch_size == 0 ? rows - consumed : self.batch_size - self.gathered;
				auto take = duckdb::MinValue<duckdb::idx_t>(room, rows - consumed);
				self.appender->Append(input, consumed, consumed + take, rows);
				self.gathered += take;
				consumed += take;
				if (self.batch_size == 0 || self.gathered >= self.batch_size) {
					self.Emit();
				}
			}
			if (consume) {
				duckdb_v2_data_chunk_destroy(chunk);
			}
		} else if (!flush) {
			// A missing chunk is allowed only to ask for a flush.
			throw duckdb::InvalidInputException("null argument 'chunk' to duckdb_v2_arrow_exporter_append");
		}
		if (flush) {
			// No further input, so whatever was gathered comes out as a short array.
			self.Emit();
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_arrow_exporter_next_array(duckdb_v2_arrow_exporter_handle exporter,
                                                    struct ArrowArray *out_array, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(exporter);
	DUCKDB_CHECK_ARG(out_array);
	out_array->release = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &self = *Convert(exporter);
		if (self.ready.empty()) {
			// Nothing ready, which the Arrow interface spells as a released array.
			return;
		}
		*out_array = self.ready.front();
		self.ready.pop_front();
	});
}

DUCKDB_V2_ERROR duckdb_v2_arrow_exporter_destroy(duckdb_v2_arrow_exporter_handle *exporter) {
	return WithErrorHandler(nullptr, [&]() {
		if (!exporter) {
			return;
		}
		if (*exporter) {
			delete Convert(*exporter);
			*exporter = nullptr;
		}
	});
}
