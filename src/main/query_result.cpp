#include "duckdb/main/query_result.hpp"

#include "duckdb/common/box_renderer.hpp"
#include "duckdb/common/column_data_collection_render_interface.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/main/buffered_data/buffered_data.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/prepared_statement_data.hpp"

namespace duckdb {

BaseQueryResult::BaseQueryResult(StatementType statement_type, StatementProperties properties_p,
                                 vector<LogicalType> types_p, vector<Identifier> names_p)
    : statement_type(statement_type), properties(std::move(properties_p)), types(std::move(types_p)),
      names(std::move(names_p)), success(true) {
	D_ASSERT(types.size() == names.size());
}

BaseQueryResult::BaseQueryResult(ErrorData error) : success(false), error(std::move(error)) {
	// Assert that the error object is initialized
	D_ASSERT(this->error.HasError());
}

BaseQueryResult::~BaseQueryResult() {
}

void BaseQueryResult::ThrowError(const string &prepended_message) const {
	D_ASSERT(HasError());
	error.Throw(prepended_message);
}

void BaseQueryResult::SetError(ErrorData error) {
	success = !error.HasError();
	this->error = std::move(error);
}

bool BaseQueryResult::HasError() const {
	D_ASSERT(error.HasError() == !success);
	return !success;
}

const ExceptionType &BaseQueryResult::GetErrorType() const {
	return error.Type();
}

const std::string &BaseQueryResult::GetError() const {
	D_ASSERT(HasError());
	return error.Message();
}

ErrorData &BaseQueryResult::GetErrorObject() {
	return error;
}

const ErrorData &BaseQueryResult::GetErrorObject() const {
	return error;
}

idx_t BaseQueryResult::ColumnCount() const {
	return types.size();
}

StatementType BaseQueryResult::GetStatementType() const {
	return statement_type;
}

const StatementProperties &BaseQueryResult::GetStatementProperties() const {
	return properties;
}

const vector<LogicalType> &BaseQueryResult::GetTypes() const {
	return types;
}

const vector<Identifier> &BaseQueryResult::GetNames() const {
	return names;
}

//===--------------------------------------------------------------------===//
// Construction
//===--------------------------------------------------------------------===//
QueryResult::QueryResult(shared_ptr<ClientContext> context_p, PreparedStatementData &statement,
                         vector<LogicalType> types_p, ClientProperties client_properties_p,
                         shared_ptr<BufferedData> buffer_p, shared_ptr<ResultFormat> format_p)
    : BaseQueryResult(statement.statement_type, statement.properties, std::move(types_p), statement.names),
      client_properties(std::move(client_properties_p)), context(std::move(context_p)), buffer(std::move(buffer_p)),
      format(std::move(format_p)) {
	if (!format) {
		format = ResultFormat::Chunk();
	}
	AdoptSettledFormat();
}

QueryResult::QueryResult(StatementType statement_type, StatementProperties properties, vector<Identifier> names_p,
                         unique_ptr<ColumnDataCollection> collection_p, ClientProperties client_properties_p)
    : BaseQueryResult(statement_type, std::move(properties), collection_p->Types(), std::move(names_p)),
      client_properties(std::move(client_properties_p)), format(ResultFormat::Chunk()),
      collection(std::move(collection_p)) {
}

QueryResult::QueryResult(StatementType statement_type, StatementProperties properties, vector<LogicalType> types_p,
                         vector<Identifier> names_p, unique_ptr<ResultUnitCollection> units_p,
                         shared_ptr<ResultFormat> format_p, shared_ptr<ResultFormatGlobalState> format_state_p,
                         ClientProperties client_properties_p)
    : BaseQueryResult(statement_type, std::move(properties), std::move(types_p), std::move(names_p)),
      client_properties(std::move(client_properties_p)), format(std::move(format_p)),
      format_state(std::move(format_state_p)), unit_collection(std::move(units_p)) {
	D_ASSERT(format && format_state && unit_collection);
}

QueryResult::QueryResult(ErrorData error)
    : BaseQueryResult(std::move(error)),
      client_properties("UTC", ArrowOffsetSize::REGULAR, false, false, false, ArrowFormatVersion::V1_0, nullptr),
      format(ResultFormat::Chunk()) {
}

QueryResult::~QueryResult() {
	Close();
}

void QueryResult::DeduplicateColumns(vector<string> &names) {
	auto identifiers = StringsToIdentifiers(names);
	DeduplicateColumns(identifiers);
	names = IdentifiersToStrings(identifiers);
}

void QueryResult::DeduplicateColumns(vector<Identifier> &names) {
	identifier_map_t<idx_t> name_map;
	for (auto &column_name : names) {
		if (name_map.find(column_name) == name_map.end()) {
			// Name does not exist yet
			name_map[column_name]++;
		} else {
			// Name already exists, we add _x where x is the repetition number
			Identifier new_column_name(column_name + "_" + std::to_string(name_map[column_name]));
			while (name_map.find(new_column_name) != name_map.end()) {
				// This name is already here due to a previous definition
				name_map[column_name]++;
				new_column_name = Identifier(column_name + "_" + std::to_string(name_map[column_name]));
			}
			column_name = new_column_name;
			name_map[new_column_name]++;
		}
	}
}

const Identifier &QueryResult::ColumnName(idx_t index) const {
	auto &names = GetNames();
	D_ASSERT(index < names.size());
	return names[index];
}

//===--------------------------------------------------------------------===//
// Execution
//===--------------------------------------------------------------------===//
unique_ptr<ClientContextLock> QueryResult::LockContext() {
	if (!context) {
		string error_str = "Attempting to execute an unsuccessful or closed query result";
		if (HasError()) {
			error_str += StringUtil::Format("\nError: %s", GetError());
		}
		throw InvalidInputException(error_str);
	}
	return context->LockContext();
}

bool QueryResult::IsOpenInternal(ClientContextLock &lock) {
	if (HasError() || !context) {
		return false;
	}
	return context->IsActiveResult(lock, *this);
}

void QueryResult::CheckExecutableInternal(ClientContextLock &lock) {
	if (!IsOpenInternal(lock)) {
		string error_str = "Attempting to execute an unsuccessful or closed query result";
		if (HasError()) {
			error_str += StringUtil::Format("\nError: %s", GetError());
		}
		throw InvalidInputException(error_str);
	}
}

bool QueryResult::IsOpen() {
	if (HasError() || !context) {
		return false;
	}
	auto lock = LockContext();
	return IsOpenInternal(*lock);
}

QueryResultState QueryResult::Cancelled() {
	if (!HasError()) {
		SetError(ErrorData(ExceptionType::INTERRUPT,
		                   "The execution of the query was cancelled before it could finish, likely caused by "
		                   "executing a different query"));
	}
	return QueryResultState::EXECUTION_ERROR;
}

QueryResultState QueryResult::Poll() {
	if (HasError()) {
		return QueryResultState::EXECUTION_ERROR;
	}
	if (IsCollected() || !context) {
		// The result was collected, or the query already ended: keep reporting the terminal state
		return QueryResultState::FINISHED;
	}
	auto lock = LockContext();
	if (!IsOpenInternal(*lock)) {
		return Cancelled();
	}
	return context->PollInternal(*lock, *this);
}

QueryResultState QueryResult::ExecuteTask() {
	auto lock = LockContext();
	CheckExecutableInternal(*lock);
	return context->ExecuteTaskInternal(*lock, *this);
}

void QueryResult::WaitForTask() {
	if (!context) {
		return;
	}
	auto lock = LockContext();
	if (!IsOpenInternal(*lock)) {
		return;
	}
	context->WaitForTask(*lock, *this);
}

void QueryResult::Close() {
	if (buffer) {
		buffer->Close();
	}
	if (context) {
		auto lock = context->LockContext();
		if (context->IsActiveResult(*lock, *this)) {
			// Abandoned before the result was consumed: release the active-query state now (matching
			// InitialCleanup) instead of leaking it until the next query or context teardown
			context->CleanupInternal(*lock, this, false);
		}
	}
	context.reset();
}

//===--------------------------------------------------------------------===//
// Format
//===--------------------------------------------------------------------===//
void QueryResult::SetFormat(shared_ptr<ResultFormat> format_p) {
	if (HasError()) {
		throw InvalidInputException("Attempting to set a format on an unsuccessful query result\nError: %s",
		                            GetError());
	}
	if (IsCollected() || !context || !buffer) {
		throw InvalidInputException("Attempting to set a format on a query result that already holds its rows");
	}
	if (buffer->Lifetime() != ResultLifetime::UNDECIDED) {
		throw InvalidInputException("Attempting to set a format on a query result that is already being %s",
		                            buffer->Lifetime() == ResultLifetime::DRAINING ? "streamed" : "materialized");
	}
	if (!format_p) {
		format_p = ResultFormat::Chunk();
	}
	format = std::move(format_p);
}

const ResultFormat &QueryResult::Format() const {
	D_ASSERT(format);
	return *format;
}

void QueryResult::AdoptSettledFormat() {
	if (!buffer || buffer->Lifetime() == ResultLifetime::UNDECIDED) {
		return;
	}
	format = buffer->SharedFormat();
	format_state = buffer->SharedFormatState();
}

void QueryResult::AdoptCollected(QueryResult &produced) {
	collection = std::move(produced.collection);
	unit_collection = std::move(produced.unit_collection);
	if (produced.format_state) {
		format = produced.format;
		format_state = produced.format_state;
	}
}

void QueryResult::ThrowFormatMismatch(const char *expected) const {
	throw InvalidInputException("This query result is in the \"%s\" format, but it was asked for the \"%s\" format",
	                            Format().Name(), expected);
}

bool QueryResult::IsChunkFormat() const {
	return Format().IsChunk();
}

const ResultFormatGlobalState &QueryResult::CheckedFormatState(const char *expected) const {
	if (!StringUtil::Equals(Format().Name(), expected)) {
		ThrowFormatMismatch(expected);
	}
	if (!format_state) {
		throw InvalidInputException("This query result has no format state yet: its format is settled by the first "
		                            "consuming call");
	}
	return *format_state;
}

void QueryResult::PrepareCollected(const char *expected) {
	Complete();
	if (HasError()) {
		throw InvalidInputException("Attempting to get collection from an unsuccessful query result\n: Error %s",
		                            GetError());
	}
	if (!StringUtil::Equals(Format().Name(), expected)) {
		ThrowFormatMismatch(expected);
	}
	if (!IsCollected()) {
		ThrowNoCollection();
	}
}

//===--------------------------------------------------------------------===//
// Retention
//===--------------------------------------------------------------------===//
void QueryResult::Materialize() {
	if (IsCollected() || HasError() || !context) {
		return;
	}
	auto lock = LockContext();
	if (!IsOpenInternal(*lock)) {
		Cancelled();
		context.reset();
		return;
	}
	D_ASSERT(buffer);
	buffer->Decide(ResultLifetime::RETAINED, format);
	AdoptSettledFormat();
}

void QueryResult::Complete() {
	if (IsCollected() || HasError() || !context) {
		return;
	}
	// The handle may hold the last reference to the context, which the lock below outlives
	auto keep_alive = context;
	auto lock = keep_alive->LockContext();
	CompleteInternal(*lock);
}

void QueryResult::CompleteInternal(ClientContextLock &lock) {
	if (IsCollected() || HasError() || !context) {
		return;
	}
	if (!IsOpenInternal(lock)) {
		Cancelled();
		context.reset();
		return;
	}
	D_ASSERT(buffer);
	buffer->Decide(ResultLifetime::RETAINED, format);
	AdoptSettledFormat();
	QueryResultState state;
	while (!IsTerminal(state = context->ExecuteTaskInternal(lock, *this))) {
		if (state == QueryResultState::BLOCKED || state == QueryResultState::READY) {
			context->WaitForTask(lock, *this);
		}
	}
	if (state == QueryResultState::FINISHED) {
		auto produced = context->GetExecutor().GetResult();
		// Cleanup can fail on an autocommit commit; it records the error on this result
		context->CleanupInternal(lock, this, false);
		if (!HasError()) {
			AdoptCollected(*produced);
		}
	}
	context.reset();
}

void QueryResult::ThrowNoCollection() const {
	throw InvalidInputException("This query result no longer holds a collection: it was taken with TakeCollection, or "
	                            "the result was closed before it was collected");
}

idx_t QueryResult::RowCount() {
	Complete();
	if (collection) {
		return collection->Count();
	}
	if (unit_collection) {
		return unit_collection->Count();
	}
	return 0;
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void QueryResult::EndQuery(ClientContextLock &lock, bool invalidate_transaction) {
	context->CleanupInternal(lock, this, invalidate_transaction);
}

void QueryResult::HandleFetchFailure(ClientContextLock &lock, ErrorData error) {
	bool invalidate_query = true;
	if (!context->ErrorInvalidatesTransaction(error.Type())) {
		// standard exceptions do not invalidate the current transaction
		invalidate_query = false;
	} else if (Exception::InvalidatesDatabase(error.Type())) {
		// fatal exceptions invalidate the entire database
		auto &db_instance = DatabaseInstance::GetDatabase(*context);
		ValidChecker::Invalidate(db_instance, error.RawMessage());
	}
	context->ProcessError(error, context->GetCurrentQuery());
	SetError(std::move(error));
	context->CleanupInternal(lock, this, invalidate_query);
}

unique_ptr<DataChunk> QueryResult::FetchRaw() {
	Complete();
	if (HasError()) {
		throw InvalidInputException("Attempting to fetch from an unsuccessful query result\nError: %s", GetError());
	}
	if (!IsChunkFormat()) {
		ThrowFormatMismatch(ChunkFormat::NAME);
	}
	if (!collection) {
		ThrowNoCollection();
	}
	auto result = make_uniq<DataChunk>();
	collection->InitializeScanChunk(*result);
	if (!scan_initialized) {
		// we disallow zero copy so the chunk is independently usable even after the result is destroyed
		collection->InitializeScan(scan_state, ColumnDataScanProperties::DISALLOW_ZERO_COPY);
		scan_initialized = true;
	}
	collection->Scan(scan_state, *result);
	if (result->size() == 0) {
		return nullptr;
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Rendering
//===--------------------------------------------------------------------===//
string QueryResult::ToString() {
	if (HasError()) {
		return GetError() + "\n";
	}
	string result = HeaderToString();
	if (!IsChunkFormat()) {
		return result + "[ Rows: " + to_string(RowCount()) + "]\n\n";
	}
	auto &coll = Collection();
	result += "[ Rows: " + to_string(coll.Count()) + "]\n";
	for (auto &row : coll.Rows()) {
		for (idx_t col_idx = 0; col_idx < coll.ColumnCount(); col_idx++) {
			if (col_idx > 0) {
				result += "\t";
			}
			auto val = row.GetValue(col_idx);
			result += val.IsNull() ? "NULL" : StringUtil::Replace(val.ToString(), string("\0", 1), "\\0");
		}
		result += "\n";
	}
	result += "\n";
	return result;
}

string QueryResult::ToBox(BoxRendererContext &context_p, const BoxRendererConfig &config) {
	if (HasError()) {
		return GetError() + "\n";
	}
	if (!IsChunkFormat()) {
		return HeaderToString() + "[ Rows: " + to_string(RowCount()) + "]\n\n";
	}
	BoxRenderer renderer(config);
	ColumnDataCollectionWrapper wrapper(Collection());
	return renderer.ToString(context_p, IdentifiersToStrings(GetNames()), wrapper);
}

bool QueryResult::Equals(QueryResult &other, bool compare_names) { // LCOV_EXCL_START
	// first compare the success state of the results
	if (HasError() != other.HasError()) {
		return false;
	}
	if (HasError()) {
		return GetErrorObject() == other.GetErrorObject();
	}
	if (!IsChunkFormat() || !other.IsChunkFormat()) {
		throw InvalidInputException("Query results can only be compared in the chunk format");
	}
	// compare names
	if (compare_names && GetNames() != other.GetNames()) {
		return false;
	}
	// compare types
	if (GetTypes() != other.GetTypes()) {
		return false;
	}
	// now compare the actual values
	// fetch chunks
	unique_ptr<DataChunk> lchunk, rchunk;
	idx_t lindex = 0, rindex = 0;
	while (true) {
		if (!lchunk || lindex == lchunk->size()) {
			lchunk = Fetch();
			lindex = 0;
		}
		if (!rchunk || rindex == rchunk->size()) {
			rchunk = other.Fetch();
			rindex = 0;
		}
		if (!lchunk && !rchunk) {
			return true;
		}
		if (!lchunk || !rchunk) {
			return false;
		}
		if (lchunk->size() == 0 && rchunk->size() == 0) {
			return true;
		}
		D_ASSERT(lchunk->ColumnCount() == rchunk->ColumnCount());
		for (; lindex < lchunk->size() && rindex < rchunk->size(); lindex++, rindex++) {
			for (idx_t col = 0; col < rchunk->ColumnCount(); col++) {
				auto lvalue = lchunk->GetValue(col, lindex);
				auto rvalue = rchunk->GetValue(col, rindex);
				if (lvalue.IsNull() && rvalue.IsNull()) {
					continue;
				}
				if (lvalue.IsNull() != rvalue.IsNull()) {
					return false;
				}
				if (lvalue != rvalue) {
					return false;
				}
			}
		}
	}
} // LCOV_EXCL_STOP

void QueryResult::Print() {
	Printer::Print(ToString());
}

string QueryResult::HeaderToString() {
	string result;
	for (auto &name : GetNames()) {
		result += name + "\t";
	}
	result += "\n";
	for (auto &type : GetTypes()) {
		result += type.ToString() + "\t";
	}
	result += "\n";
	return result;
}

} // namespace duckdb
