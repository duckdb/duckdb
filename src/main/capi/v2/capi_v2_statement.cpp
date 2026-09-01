#include "duckdb/main/capi_v2/capi_v2_internal.hpp"

#include "duckdb/parser/sql_statement.hpp"

namespace duckdb::capiv2 {
namespace {

struct StatementIteratorWrapperV2 {
	StatementIteratorWrapperV2(shared_ptr<ClientContext> context_p, string query_p)
	    : context(std::move(context_p)), query(std::move(query_p)) {
	}
	shared_ptr<ClientContext> context;
	//! The original query string, kept to render a parse error through ProcessError.
	string query;
	//! Constructed on the first next(): the ParseIterator constructor runs UTF-8
	//! validation and can throw, and that must route through the same parse-error
	//! rendering as a per-statement syntax error.
	unique_ptr<ParseIterator> iterator;
	//! Set once iteration is spent, by clean exhaustion or by a parse error. A spent
	//! iterator yields nothing further: next() returns a NULL statement idempotently.
	bool finished = false;
};

} // namespace

auto Convert(duckdb_v2_statement_iterator_handle ptr) -> StatementIteratorWrapperV2 * {
	return reinterpret_cast<StatementIteratorWrapperV2 *>(ptr);
}
auto Convert(StatementIteratorWrapperV2 *ptr) -> duckdb_v2_statement_iterator_handle {
	return reinterpret_cast<duckdb_v2_statement_iterator_handle>(ptr);
}

} // namespace duckdb::capiv2

//----------------------------------------------------------------------------------------------------------------------
// Public API
//----------------------------------------------------------------------------------------------------------------------

using namespace duckdb::capiv2;

DUCKDB_V2_ERROR duckdb_v2_parse_sql(duckdb_v2_connection_handle conn, const char *sql,
                                    duckdb_v2_statement_iterator_handle *out_iterator,
                                    duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(conn);
	DUCKDB_CHECK_ARG(sql);
	DUCKDB_CHECK_ARG(out_iterator);
	*out_iterator = nullptr;
	return WithErrorHandler(err, [&]() {
		auto *connection = Convert(conn);
		// Set up a lazy iterator over the connection's parser options and
		// extensions, but parse nothing here. Each statement is parsed on demand by
		// statement_iterator_next, so a statement that registers grammar (LOAD an
		// extension) is parsed and executed before a following statement uses it.
		// Statements are raw parser output: statement-level rewrites (pragma
		// reparsing, expansion unpacking, transaction wrapping) happen in
		// statement_execute, so a statement group is never split across the API
		// boundary. A parse error is not raised here; it surfaces from the next()
		// that reaches the failing statement.
		auto wrapper = duckdb::make_uniq<StatementIteratorWrapperV2>(connection->context, std::string(sql));
		*out_iterator = Convert(wrapper.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_statement_iterator_next(duckdb_v2_statement_iterator_handle iterator,
                                                  duckdb_v2_sql_statement_handle *out_statement,
                                                  duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(iterator);
	DUCKDB_CHECK_ARG(out_statement);
	*out_statement = nullptr;
	return WithErrorHandler(err, [&]() {
		auto *wrapper = Convert(iterator);
		if (wrapper->finished) {
			// Spent by a prior exhaustion or parse error: *out_statement stays NULL.
			return;
		}
		duckdb::unique_ptr<duckdb::SQLStatement> statement;
		try {
			// Construct the ParseIterator on first use: its constructor runs UTF-8
			// validation and can throw, routed through the same rendering below.
			if (!wrapper->iterator) {
				wrapper->iterator = duckdb::make_uniq<duckdb::ParseIterator>(*wrapper->context, wrapper->query);
			}
			// Parse the next statement on demand. Peek returns false at exhaustion;
			// *out_statement then stays NULL, idempotently.
			if (!wrapper->iterator->Peek()) {
				wrapper->finished = true;
				return;
			}
			statement = wrapper->iterator->GetStatement();
		} catch (const std::exception &ex) {
			// A parse error surfaces here, when iteration reaches the failing
			// statement, not up front. It terminates iteration (a following next()
			// reports clean exhaustion, not the error again). Route it through the
			// engine's public ProcessError like the eager query path so the rendered
			// shape honors errors_as_json (JSON, else LINE/caret), then re-throw to
			// route back through WithErrorHandler.
			wrapper->finished = true;
			duckdb::ErrorData error(ex);
			wrapper->context->ProcessError(error, wrapper->query);
			error.Throw();
		}
		*out_statement = Convert(statement.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_sql_statement_destroy(duckdb_v2_sql_statement_handle *statement) {
	return WithErrorHandler(nullptr, [&]() {
		if (!statement) {
			return;
		}
		if (*statement) {
			delete Convert(*statement);
			*statement = nullptr;
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_statement_iterator_destroy(duckdb_v2_statement_iterator_handle *iterator) {
	return WithErrorHandler(nullptr, [&]() {
		if (!iterator) {
			return;
		}
		if (*iterator) {
			delete Convert(*iterator);
			*iterator = nullptr;
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_statement_bind(duckdb_v2_connection_handle conn, duckdb_v2_sql_statement_handle statement,
                                         duckdb_v2_schema_handle *out_schema, duckdb_v2_schema_handle *out_parameters,
                                         duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(conn);
	DUCKDB_CHECK_ARG(statement);
	DUCKDB_CHECK_ARG(out_schema);
	*out_schema = nullptr;
	return WithErrorHandler(err, [&]() {
		if (out_parameters) {
			*out_parameters = nullptr;
		}

		auto *connection = Convert(conn);
		// Borrowed, not consumed: bind a copy. Preprocess it so the schema matches
		// what execution would bind (pragma reparse, expansion).
		duckdb::vector<duckdb::unique_ptr<duckdb::SQLStatement>> fragments;
		fragments.push_back(Convert(statement)->Copy());
		connection->context->PreprocessStatements(fragments);
		if (fragments.size() != 1) {
			// A statement that expands into a group (a dynamic PIVOT, or
			// statement-expanding DDL) is not bindable: bind binds one engine statement.
			throw duckdb::InvalidInputException(
			    "statement expands into multiple engine statements; bind is not supported, execute it instead");
		}
		// BindStatement, not Prepare: bind phase only (read-only, no optimize / physical
		// plan) and must not cancel a live result (Prepare's InitialCleanup would). It
		// throws on a bind error, routed through WithErrorHandler.
		auto signature = connection->context->BindStatement(std::move(fragments[0]));
		// Assemble into locals, publish to the out-params only at the end once nothing
		// can throw, so a failure leaves them NULL.
		auto output = duckdb::make_uniq<CV2Schema>();
		for (duckdb::idx_t i = 0; i < signature.types.size(); i++) {
			output->fields.push_back({signature.names[i].GetIdentifierName(), signature.types[i]});
		}

		duckdb::unique_ptr<CV2Schema> params;
		if (out_parameters) {
			params = duckdb::make_uniq<CV2Schema>();
			// BindStatement returns parameters unordered; order by binding index for the
			// positional input-schema contract that pairs with statement_execute's values.
			// Positional params may be gapped ($1, $3 with no $2), so not a dense 1..N; the
			// field name carries the binding index.
			std::sort(signature.parameters.begin(), signature.parameters.end(),
			          [](const auto &a, const auto &b) { return a.index < b.index; });
			for (auto &parameter : signature.parameters) {
				params->fields.push_back({parameter.identifier.GetIdentifierName(), parameter.type});
			}
		}
		*out_schema = Convert(output.release());
		if (out_parameters) {
			*out_parameters = Convert(params.release());
		}
	});
}
