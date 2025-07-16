#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "fmt/format.h"

using duckdb::ArrowConverter;
using duckdb::ArrowResultWrapper;
using duckdb::CClientPropertiesWrapper;
using duckdb::Connection;
using duckdb::DataChunk;
using duckdb::LogicalType;
using duckdb::MaterializedQueryResult;
using duckdb::PreparedStatementWrapper;
using duckdb::QueryResult;
using duckdb::QueryResultType;

duckdb_error_data duckdb_to_arrow_schema(duckdb_client_properties client_properties, duckdb_logical_type *types,
                                         char **names, idx_t column_count, duckdb_arrow_schema *out_schema) {

	if (!types || !names || !out_schema || !client_properties) {
		return duckdb_create_error_data(DUCKDB_ERROR_INVALID_INPUT, "Invalid argument(s) to duckdb_to_arrow_schema");
	}
	duckdb::vector<LogicalType> schema_types;
	duckdb::vector<std::string> schema_names;
	for (idx_t i = 0; i < column_count; i++) {
		schema_names.emplace_back(names[i]);
		schema_types.emplace_back(*reinterpret_cast<duckdb::LogicalType *>(types[i]));
	}
	auto client_properties_wrapper = reinterpret_cast<CClientPropertiesWrapper *>(client_properties);
	try {
		ArrowConverter::ToArrowSchema(reinterpret_cast<ArrowSchema *>(*out_schema), schema_types, schema_names,
		                              client_properties_wrapper->properties);
	} catch (const duckdb::Exception &ex) {
		return duckdb_create_error_data(DUCKDB_ERROR_INVALID_INPUT, ex.what());
	} catch (const std::exception &ex) {
		return duckdb_create_error_data(DUCKDB_ERROR_INVALID_INPUT, ex.what());
	} catch (...) {
		return duckdb_create_error_data(DUCKDB_ERROR_INVALID_INPUT, "Unknown error occurred during conversion");
	}
	return nullptr;
}

duckdb_error_data duckdb_data_chunk_to_arrow(duckdb_client_properties client_properties, duckdb_data_chunk chunk,
                                             duckdb_arrow_array *out_arrow_array) {
	if (!client_properties || !out_arrow_array || !chunk) {
		return duckdb_create_error_data(DUCKDB_ERROR_INVALID_INPUT,
		                                "Invalid argument(s) to duckdb_data_chunk_to_arrow");
	}
	auto dchunk = reinterpret_cast<duckdb::DataChunk *>(chunk);
	auto client_properties_wrapper = reinterpret_cast<CClientPropertiesWrapper *>(client_properties);
	auto extension_type_cast = duckdb::ArrowTypeExtensionData::GetExtensionTypes(
	    *client_properties_wrapper->properties.client_context, dchunk->GetTypes());

	try {
		ArrowConverter::ToArrowArray(*dchunk, reinterpret_cast<ArrowArray *>(*out_arrow_array),
		                             client_properties_wrapper->properties, extension_type_cast);
	} catch (const duckdb::Exception &ex) {
		return duckdb_create_error_data(DUCKDB_ERROR_INVALID_INPUT, ex.what());
	} catch (const std::exception &ex) {
		return duckdb_create_error_data(DUCKDB_ERROR_INVALID_INPUT, ex.what());
	} catch (...) {
		return duckdb_create_error_data(DUCKDB_ERROR_INVALID_INPUT, "Unknown error occurred during conversion");
	}
	return nullptr;
}

duckdb_error_data arrow_to_duckdb_schema(duckdb_connection connection, duckdb_arrow_schema schema,
                                         duckdb_arrow_converted_schema *out_types, char ***out_names,
                                         idx_t *out_column_count) {
	if (!connection || !schema || !out_types || !out_names || !out_column_count) {
		return duckdb_create_error_data(DUCKDB_ERROR_INVALID_INPUT,
		                                "Invalid argument(s) to duckdb_data_chunk_to_arrow");
	}
	duckdb::vector<std::string> names;
	auto conn = reinterpret_cast<Connection *>(connection);
	auto arrow_table = new duckdb::ArrowTableType();
	*out_types = reinterpret_cast<duckdb_arrow_converted_schema>(arrow_table);
	try {
		duckdb::vector<LogicalType> return_types;
		duckdb::ArrowTableFunction::PopulateArrowTableType(duckdb::DBConfig::GetConfig(*conn->context), *arrow_table,
		                                                   *reinterpret_cast<ArrowSchema *>(schema), names,
		                                                   return_types);
		QueryResult::DeduplicateColumns(names);
	} catch (const duckdb::Exception &ex) {
		delete arrow_table;
		return duckdb_create_error_data(DUCKDB_ERROR_INVALID_INPUT, ex.what());
	} catch (const std::exception &ex) {
		delete arrow_table;
		return duckdb_create_error_data(DUCKDB_ERROR_INVALID_INPUT, ex.what());
	} catch (...) {
		delete arrow_table;
		return duckdb_create_error_data(DUCKDB_ERROR_INVALID_INPUT, "Unknown error occurred during conversion");
	}
	const idx_t column_count = names.size();
	*out_column_count = column_count;
	*out_names = new char *[column_count];
	for (idx_t i = 0; i < column_count; i++) {
		(*out_names)[i] = new char[names[i].size() + 1];
		std::strcpy((*out_names)[i], names[i].c_str());
	}
	return nullptr;
}

duckdb_error_data arrow_to_duckdb_data_chunk(duckdb_connection connection, duckdb_arrow_array arrow_array,
                                             duckdb_arrow_converted_schema converted_schema,
                                             duckdb_data_chunk *out_chunk) {
	if (!connection || !arrow_array || !converted_schema || !out_chunk) {
		return duckdb_create_error_data(DUCKDB_ERROR_INVALID_INPUT,
		                                "Invalid argument(s) to duckdb_data_chunk_to_arrow");
	}
	auto arrow_array_cpp = reinterpret_cast<ArrowArray *>(arrow_array);
	auto arrow_table = reinterpret_cast<duckdb::ArrowTableType *>(converted_schema);
	auto conn = reinterpret_cast<Connection *>(connection);
	auto &type_vector = arrow_table->GetTypes();
	std::vector<duckdb::LogicalType *> types_ptr(type_vector.size());
	for (idx_t i = 0; i < type_vector.size(); i++) {
		types_ptr[i] = &type_vector[i];
	}

	*out_chunk = duckdb_create_data_chunk(reinterpret_cast<duckdb_logical_type *>(&types_ptr[0]), types_ptr.size());
	auto &arrow_types = arrow_table->GetColumns();
	auto output_size =
	    duckdb::MinValue<idx_t>(STANDARD_VECTOR_SIZE, duckdb::NumericCast<idx_t>(arrow_array_cpp->length));
	auto dchunk = reinterpret_cast<duckdb::DataChunk *>(*out_chunk);

	dchunk->SetCardinality(output_size);
	for (idx_t i = 0; i < dchunk->ColumnCount(); i++) {
		auto &parent_array = *arrow_array_cpp;
		auto &array = parent_array.children[i];
		if (array->length > STANDARD_VECTOR_SIZE) {
			return duckdb_create_error_data(DUCKDB_ERROR_NOT_IMPLEMENTED,
			                                "We only support Arrow chunks of up to 2048 rows");
		}
		auto arrow_type = arrow_types.at(i);
		auto array_physical_type = arrow_type->GetPhysicalType();
		auto array_state = duckdb::make_uniq<duckdb::ArrowArrayScanState>(*conn->context);
		// We need to make sure that our chunk will hold ze ownership
		array_state->owned_data = duckdb::make_shared_ptr<duckdb::ArrowArrayWrapper>();
		array_state->owned_data->arrow_array = *arrow_array_cpp;
		// We set it to nullptr to effectively transfer ze ownership
		arrow_array_cpp->release = nullptr;

		switch (array_physical_type) {
		case duckdb::ArrowArrayPhysicalType::DEFAULT:
			duckdb::ArrowToDuckDBConversion::SetValidityMask(dchunk->data[i], *array, 0, dchunk->size(),
			                                                 parent_array.offset, -1);

			duckdb::ArrowToDuckDBConversion::ColumnArrowToDuckDB(dchunk->data[i], *array, 0, *array_state,
			                                                     dchunk->size(), *arrow_type);
			break;
		default:
			return duckdb_create_error_data(DUCKDB_ERROR_NOT_IMPLEMENTED,
			                                "Only Default Physical Types are currently supported");
		}
	}
	return nullptr;
}
void duckdb_destroy_arrow_schema(duckdb_arrow_schema *arrow_schema) {
	if (arrow_schema && *arrow_schema) {
		auto schema = reinterpret_cast<ArrowSchema *>(*arrow_schema);
		if (schema->release) {
			schema->release(schema);
		}
		delete schema;
		*arrow_schema = nullptr;
	}
}

void duckdb_destroy_arrow_converted_schema(duckdb_arrow_converted_schema *arrow_converted_schema) {
	if (arrow_converted_schema && *arrow_converted_schema) {
		auto converted_schema = reinterpret_cast<duckdb::ArrowTableType *>(*arrow_converted_schema);
		delete converted_schema;
		*arrow_converted_schema = nullptr;
	}
}

void duckdb_destroy_arrow_array(duckdb_arrow_array *arrow_array) {
	if (arrow_array && *arrow_array) {
		auto array = reinterpret_cast<ArrowArray *>(*arrow_array);
		if (array->release) {
			array->release(array);
		}
		delete array;
		*arrow_array = nullptr;
	}
}

duckdb_arrow_array duckdb_create_arrow_array() {
	auto result = new ArrowArray();
	return reinterpret_cast<duckdb_arrow_array>(result);
}

duckdb_arrow_schema duckdb_create_arrow_schema() {
	auto result = new ArrowSchema();
	return reinterpret_cast<duckdb_arrow_schema>(result);
}

duckdb_state duckdb_query_arrow(duckdb_connection connection, const char *query, duckdb_arrow *out_result) {
	Connection *conn = (Connection *)connection;
	auto wrapper = new ArrowResultWrapper();
	wrapper->result = conn->Query(query);
	*out_result = (duckdb_arrow)wrapper;
	return !wrapper->result->HasError() ? DuckDBSuccess : DuckDBError;
}

duckdb_state duckdb_query_arrow_schema(duckdb_arrow result, duckdb_arrow_schema *out_schema) {
	if (!out_schema) {
		return DuckDBSuccess;
	}
	auto wrapper = reinterpret_cast<ArrowResultWrapper *>(result);
	try {
		ArrowConverter::ToArrowSchema((ArrowSchema *)*out_schema, wrapper->result->types, wrapper->result->names,
		                              wrapper->result->client_properties);
	} catch (...) {
		return DuckDBError;
	}
	return DuckDBSuccess;
}

duckdb_state duckdb_prepared_arrow_schema(duckdb_prepared_statement prepared, duckdb_arrow_schema *out_schema) {
	if (!out_schema) {
		return DuckDBSuccess;
	}
	auto wrapper = reinterpret_cast<PreparedStatementWrapper *>(prepared);
	if (!wrapper || !wrapper->statement || !wrapper->statement->data) {
		return DuckDBError;
	}
	auto properties = wrapper->statement->context->GetClientProperties();
	duckdb::vector<duckdb::LogicalType> prepared_types;
	duckdb::vector<duckdb::string> prepared_names;

	auto count = wrapper->statement->data->properties.parameter_count;
	for (idx_t i = 0; i < count; i++) {
		// Every prepared parameter type is UNKNOWN, which we need to map to NULL according to the spec of
		// 'AdbcStatementGetParameterSchema'
		const auto type = LogicalType::SQLNULL;

		// FIXME: we don't support named parameters yet, but when we do, this needs to be updated
		auto name = std::to_string(i);
		prepared_types.push_back(type);
		prepared_names.push_back(name);
	}

	auto result_schema = (ArrowSchema *)*out_schema;
	if (!result_schema) {
		return DuckDBError;
	}

	if (result_schema->release) {
		// Need to release the existing schema before we overwrite it
		result_schema->release(result_schema);
		D_ASSERT(!result_schema->release);
	}

	ArrowConverter::ToArrowSchema(result_schema, prepared_types, prepared_names, properties);
	return DuckDBSuccess;
}

duckdb_state duckdb_query_arrow_array(duckdb_arrow result, duckdb_arrow_array *out_array) {
	if (!out_array) {
		return DuckDBSuccess;
	}
	auto wrapper = reinterpret_cast<ArrowResultWrapper *>(result);
	auto success = wrapper->result->TryFetch(wrapper->current_chunk, wrapper->result->GetErrorObject());
	if (!success) { // LCOV_EXCL_START
		return DuckDBError;
	} // LCOV_EXCL_STOP
	if (!wrapper->current_chunk || wrapper->current_chunk->size() == 0) {
		return DuckDBSuccess;
	}
	auto extension_type_cast = duckdb::ArrowTypeExtensionData::GetExtensionTypes(
	    *wrapper->result->client_properties.client_context, wrapper->result->types);
	ArrowConverter::ToArrowArray(*wrapper->current_chunk, reinterpret_cast<ArrowArray *>(*out_array),
	                             wrapper->result->client_properties, extension_type_cast);
	return DuckDBSuccess;
}

void duckdb_result_arrow_array(duckdb_result result, duckdb_data_chunk chunk, duckdb_arrow_array *out_array) {
	if (!out_array) {
		return;
	}
	auto dchunk = reinterpret_cast<duckdb::DataChunk *>(chunk);
	auto &result_data = *(reinterpret_cast<duckdb::DuckDBResultData *>(result.internal_data));
	auto extension_type_cast = duckdb::ArrowTypeExtensionData::GetExtensionTypes(
	    *result_data.result->client_properties.client_context, result_data.result->types);

	ArrowConverter::ToArrowArray(*dchunk, reinterpret_cast<ArrowArray *>(*out_array),
	                             result_data.result->client_properties, extension_type_cast);
}

idx_t duckdb_arrow_row_count(duckdb_arrow result) {
	auto wrapper = reinterpret_cast<ArrowResultWrapper *>(result);
	if (wrapper->result->HasError()) {
		return 0;
	}
	return wrapper->result->RowCount();
}

idx_t duckdb_arrow_column_count(duckdb_arrow result) {
	auto wrapper = reinterpret_cast<ArrowResultWrapper *>(result);
	return wrapper->result->ColumnCount();
}

idx_t duckdb_arrow_rows_changed(duckdb_arrow result) {
	auto wrapper = reinterpret_cast<ArrowResultWrapper *>(result);
	if (wrapper->result->HasError()) {
		return 0;
	}
	idx_t rows_changed = 0;
	auto &collection = wrapper->result->Collection();
	idx_t row_count = collection.Count();
	if (row_count > 0 && wrapper->result->properties.return_type == duckdb::StatementReturnType::CHANGED_ROWS) {
		auto rows = collection.GetRows();
		D_ASSERT(row_count == 1);
		D_ASSERT(rows.size() == 1);
		rows_changed = duckdb::NumericCast<idx_t>(rows[0].GetValue(0).GetValue<int64_t>());
	}
	return rows_changed;
}

const char *duckdb_query_arrow_error(duckdb_arrow result) {
	auto wrapper = reinterpret_cast<ArrowResultWrapper *>(result);
	return wrapper->result->GetError().c_str();
}

void duckdb_destroy_arrow(duckdb_arrow *result) {
	if (*result) {
		auto wrapper = reinterpret_cast<ArrowResultWrapper *>(*result);
		delete wrapper;
		*result = nullptr;
	}
}

void duckdb_destroy_arrow_stream(duckdb_arrow_stream *stream_p) {

	auto stream = reinterpret_cast<ArrowArrayStream *>(*stream_p);
	if (!stream) {
		return;
	}
	if (stream->release) {
		stream->release(stream);
	}
	D_ASSERT(!stream->release);

	delete stream;
	*stream_p = nullptr;
}

duckdb_state duckdb_execute_prepared_arrow(duckdb_prepared_statement prepared_statement, duckdb_arrow *out_result) {
	auto wrapper = reinterpret_cast<PreparedStatementWrapper *>(prepared_statement);
	if (!wrapper || !wrapper->statement || wrapper->statement->HasError() || !out_result) {
		return DuckDBError;
	}
	auto arrow_wrapper = new ArrowResultWrapper();
	auto result = wrapper->statement->Execute(wrapper->values, false);
	D_ASSERT(result->type == QueryResultType::MATERIALIZED_RESULT);
	arrow_wrapper->result = duckdb::unique_ptr_cast<QueryResult, MaterializedQueryResult>(std::move(result));
	*out_result = reinterpret_cast<duckdb_arrow>(arrow_wrapper);
	return !arrow_wrapper->result->HasError() ? DuckDBSuccess : DuckDBError;
}

namespace arrow_array_stream_wrapper {
namespace {
struct PrivateData {
	ArrowSchema *schema;
	ArrowArray *array;
	bool done = false;
};

// LCOV_EXCL_START
// This function is never called, but used to set ArrowSchema's release functions to a non-null NOOP.
void EmptySchemaRelease(ArrowSchema *schema) {
	schema->release = nullptr;
}
// LCOV_EXCL_STOP

void EmptyArrayRelease(ArrowArray *array) {
	array->release = nullptr;
}

void EmptyStreamRelease(ArrowArrayStream *stream) {
	stream->release = nullptr;
}

void FactoryGetSchema(ArrowArrayStream *stream, ArrowSchema &schema) {
	stream->get_schema(stream, &schema);

	// Need to nullify the root schema's release function here, because streams don't allow us to set the release
	// function. For the schema's children, we nullify the release functions in `duckdb_arrow_scan`, so we don't need to
	// handle them again here. We set this to nullptr and not EmptySchemaRelease to prevent ArrowSchemaWrapper's
	// destructor from destroying the schema (it's the caller's responsibility).
	schema.release = nullptr;
}

int GetSchema(struct ArrowArrayStream *stream, struct ArrowSchema *out) {
	auto private_data = static_cast<arrow_array_stream_wrapper::PrivateData *>((stream->private_data));
	if (private_data->schema == nullptr) {
		return DuckDBError;
	}

	*out = *private_data->schema;
	out->release = EmptySchemaRelease;
	return DuckDBSuccess;
}

int GetNext(struct ArrowArrayStream *stream, struct ArrowArray *out) {
	auto private_data = static_cast<arrow_array_stream_wrapper::PrivateData *>((stream->private_data));
	*out = *private_data->array;
	if (private_data->done) {
		out->release = nullptr;
	} else {
		out->release = EmptyArrayRelease;
	}

	private_data->done = true;
	return DuckDBSuccess;
}

duckdb::unique_ptr<duckdb::ArrowArrayStreamWrapper> FactoryGetNext(uintptr_t stream_factory_ptr,
                                                                   duckdb::ArrowStreamParameters &parameters) {
	auto stream = reinterpret_cast<ArrowArrayStream *>(stream_factory_ptr);
	auto ret = duckdb::make_uniq<duckdb::ArrowArrayStreamWrapper>();
	ret->arrow_array_stream = *stream;
	ret->arrow_array_stream.release = EmptyStreamRelease;
	return ret;
}

// LCOV_EXCL_START
// This function is never be called, because it's used to construct a stream wrapping around a caller-supplied
// ArrowArray. Thus, the stream itself cannot produce an error.
const char *GetLastError(struct ArrowArrayStream *stream) {
	return nullptr;
}
// LCOV_EXCL_STOP

void Release(struct ArrowArrayStream *stream) {
	if (stream->private_data != nullptr) {
		delete reinterpret_cast<PrivateData *>(stream->private_data);
	}

	stream->private_data = nullptr;
	stream->release = nullptr;
}

duckdb_state Ingest(duckdb_connection connection, const char *table_name, struct ArrowArrayStream *input) {
	try {
		auto cconn = reinterpret_cast<duckdb::Connection *>(connection);
		cconn
		    ->TableFunction("arrow_scan", {duckdb::Value::POINTER((uintptr_t)input),
		                                   duckdb::Value::POINTER((uintptr_t)FactoryGetNext),
		                                   duckdb::Value::POINTER((uintptr_t)FactoryGetSchema)})
		    ->CreateView(table_name, true, false);
	} catch (...) { // LCOV_EXCL_START
		// Tried covering this in tests, but it proved harder than expected. At the time of writing:
		// - Passing any name to `CreateView` worked without throwing an exception
		// - Passing a null Arrow array worked without throwing an exception
		// - Passing an invalid schema (without any columns) led to an InternalException with SIGABRT, which is meant to
		//   be un-catchable. This case likely needs to be handled gracefully within `arrow_scan`.
		// Ref: https://discord.com/channels/909674491309850675/921100573732909107/1115230468699336785
		return DuckDBError;
	} // LCOV_EXCL_STOP

	return DuckDBSuccess;
}
} // namespace
} // namespace arrow_array_stream_wrapper

duckdb_state duckdb_arrow_scan(duckdb_connection connection, const char *table_name, duckdb_arrow_stream arrow) {
	auto stream = reinterpret_cast<ArrowArrayStream *>(arrow);

	// Backup release functions - we nullify children schema release functions because we don't want to release on
	// behalf of the caller, downstream in our code. Note that Arrow releases target immediate children, but aren't
	// recursive. So we only back up immediate children here and restore their functions.
	ArrowSchema schema;
	if (stream->get_schema(stream, &schema) == DuckDBError) {
		return DuckDBError;
	}

	typedef void (*release_fn_t)(ArrowSchema *);
	std::vector<release_fn_t> release_fns(duckdb::NumericCast<idx_t>(schema.n_children));
	for (idx_t i = 0; i < duckdb::NumericCast<idx_t>(schema.n_children); i++) {
		auto child = schema.children[i];
		release_fns[i] = child->release;
		child->release = arrow_array_stream_wrapper::EmptySchemaRelease;
	}

	auto ret = arrow_array_stream_wrapper::Ingest(connection, table_name, stream);

	// Restore release functions.
	for (idx_t i = 0; i < duckdb::NumericCast<idx_t>(schema.n_children); i++) {
		schema.children[i]->release = release_fns[i];
	}

	return ret;
}

duckdb_state duckdb_arrow_array_scan(duckdb_connection connection, const char *table_name,
                                     duckdb_arrow_schema arrow_schema, duckdb_arrow_array arrow_array,
                                     duckdb_arrow_stream *out_stream) {
	auto private_data = new arrow_array_stream_wrapper::PrivateData;
	private_data->schema = reinterpret_cast<ArrowSchema *>(arrow_schema);
	private_data->array = reinterpret_cast<ArrowArray *>(arrow_array);
	private_data->done = false;

	ArrowArrayStream *stream = new ArrowArrayStream;
	*out_stream = reinterpret_cast<duckdb_arrow_stream>(stream);
	stream->get_schema = arrow_array_stream_wrapper::GetSchema;
	stream->get_next = arrow_array_stream_wrapper::GetNext;
	stream->get_last_error = arrow_array_stream_wrapper::GetLastError;
	stream->release = arrow_array_stream_wrapper::Release;
	stream->private_data = private_data;

	return duckdb_arrow_scan(connection, table_name, reinterpret_cast<duckdb_arrow_stream>(stream));
}
