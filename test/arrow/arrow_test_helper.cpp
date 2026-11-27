#include "arrow/arrow_test_helper.hpp"
#include "duckdb/common/enums/set_operation_type.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/tableref/column_data_ref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

duckdb::unique_ptr<duckdb::ArrowArrayStreamWrapper>
ArrowStreamTestFactory::ProduceStream(duckdb::ArrowStreamParameters &parameters) {
	auto stream_wrapper = duckdb::make_uniq<duckdb::ArrowArrayStreamWrapper>();
	stream_wrapper->arrow_array_stream = stream.get();

	return stream_wrapper;
}

void ArrowStreamTestFactory::GetSchema(ArrowSchema &schema) {
	stream.get().get_schema(&stream.get(), &schema);
}

namespace duckdb {

int ArrowTestFactory::ArrowArrayStreamGetSchema(struct ArrowArrayStream *stream, struct ArrowSchema *out) {
	if (!stream->private_data) {
		throw InternalException("No private data!?");
	}
	auto &data = *((ArrowArrayStreamData *)stream->private_data);
	data.factory.ToArrowSchema(out);
	return 0;
}

static int NextFromMaterialized(QueryResult &res, ClientProperties properties, struct ArrowArray *out) {
	unordered_map<idx_t, const duckdb::shared_ptr<ArrowTypeExtensionData>> extension_type_cast;
	auto chunk = res.Fetch();
	if (!chunk || chunk->size() == 0) {
		return 0;
	}
	ArrowConverter::ToArrowArray(*chunk, out, properties, extension_type_cast);
	return 0;
}

static int NextFromArrow(QueryResult &res, struct ArrowArray *out) {
	auto array = res.Fetch<ArrowFormat>();
	if (!array) {
		return 0;
	}
	array->MoveTo(*out);
	return 0;
}

int ArrowTestFactory::ArrowArrayStreamGetNext(struct ArrowArrayStream *stream, struct ArrowArray *out) {
	if (!stream->private_data) {
		throw InternalException("No private data!?");
	}
	auto &data = *((ArrowArrayStreamData *)stream->private_data);
	auto &result = *data.factory.result;
	if (result.Format().Is<ChunkFormat>()) {
		return NextFromMaterialized(result, data.options, out);
	}
	return NextFromArrow(result, out);
}

const char *ArrowTestFactory::ArrowArrayStreamGetLastError(struct ArrowArrayStream *stream) {
	throw InternalException("Error!?!!");
}

void ArrowTestFactory::ArrowArrayStreamRelease(struct ArrowArrayStream *stream) {
	if (!stream || !stream->private_data) {
		return;
	}
	auto data = (ArrowArrayStreamData *)stream->private_data;
	delete data;
	stream->private_data = nullptr;
	stream->release = nullptr;
}

duckdb::unique_ptr<duckdb::ArrowArrayStreamWrapper> ArrowTestFactory::ProduceStream(ArrowStreamParameters &parameters) {
	auto &factory = *this;
	if (!factory.result) {
		throw InternalException("Stream already consumed!");
	}

	auto stream_wrapper = make_uniq<ArrowArrayStreamWrapper>();
	auto private_data = make_uniq<ArrowArrayStreamData>(factory, factory.options);
	stream_wrapper->arrow_array_stream.get_schema = ArrowArrayStreamGetSchema;
	stream_wrapper->arrow_array_stream.get_next = ArrowArrayStreamGetNext;
	stream_wrapper->arrow_array_stream.get_last_error = ArrowArrayStreamGetLastError;
	stream_wrapper->arrow_array_stream.release = ArrowArrayStreamRelease;
	stream_wrapper->arrow_array_stream.private_data = private_data.release();

	return stream_wrapper;
}

void ArrowTestFactory::GetSchema(ArrowSchema &schema) {
	ToArrowSchema(&schema);
}

void ArrowTestFactory::ToArrowSchema(struct ArrowSchema *out) {
	ArrowConverter::ToArrowSchema(out, types, names, options);
}

static unique_ptr<TableRef> ArrowScanRef(shared_ptr<ArrowScanFactory> factory) {
	// construct the `arrow_scan(...)` function expression
	auto scan = make_uniq<TableFunctionRef>();
	scan->function = make_uniq<FunctionExpression>("arrow_scan", vector<unique_ptr<ParsedExpression>>());
	scan->bind_info = std::move(factory);
	return std::move(scan);
}

unique_ptr<SelectStatement> ArrowTestHelper::ArrowScanSelect(shared_ptr<ArrowScanFactory> factory) {
	// construct the `SELECT * FROM arrow_scan(...)` statement
	auto node = make_uniq<SelectNode>();
	node->select_list.push_back(make_uniq<StarExpression>());
	node->from_table = ArrowScanRef(std::move(factory));
	auto select = make_uniq<SelectStatement>();
	select->node = std::move(node);
	return select;
}

void ArrowTestHelper::CreateArrowScanView(Connection &con, shared_ptr<ArrowScanFactory> factory, const string &name,
                                          bool replace, bool temporary) {
	CreateViewOver(con, ArrowScanRef(std::move(factory)), name, replace, temporary);
}

unique_ptr<QueryResult> ArrowTestHelper::ScanArrowObject(Connection &con, shared_ptr<ArrowScanFactory> factory) {
	auto arrow_result = con.Query(ArrowScanSelect(std::move(factory)));
	if (arrow_result->HasError()) {
		printf("-------------------------------------\n");
		printf("Arrow round-trip query error: %s\n", arrow_result->GetError().c_str());
		printf("-------------------------------------\n");
		printf("-------------------------------------\n");
		arrow_result->ThrowError();
	}
	return arrow_result;
}

bool ArrowTestHelper::CompareResults(Connection &con, shared_ptr<ArrowScanFactory> factory, const string &query) {
	// run FROM arrow_scan(...) EXCEPT ALL <query> - this should be empty
	unique_ptr<QueryNode> regular_result;
	auto statements = con.ExtractStatements(query);
	if (statements.size() != 1 || statements[0]->type != StatementType::SELECT_STATEMENT) {
		auto query_result = con.Query(query);
		shared_ptr<ColumnDataCollection> duck_collection = query_result->TakeCollection();
		auto names = query_result->GetNames();
		QueryResult::DeduplicateColumns(names);
		auto duck = make_uniq<ColumnDataRef>(std::move(duck_collection), std::move(names));
		duck->alias = Identifier("duck");
		auto node = make_uniq<SelectNode>();
		node->select_list.push_back(make_uniq<StarExpression>());
		node->from_table = std::move(duck);
		regular_result = std::move(node);
	} else {
		regular_result = std::move(statements[0]->Cast<SelectStatement>().node);
	}

	auto except = make_uniq<SetOperationNode>();
	except->children.push_back(std::move(ArrowScanSelect(std::move(factory))->node));
	except->children.push_back(std::move(regular_result));
	except->setop_type = SetOperationType::EXCEPT;
	except->setop_all = true;
	auto select = make_uniq<SelectStatement>();
	select->node = std::move(except);
	auto result = con.Query(std::move(select));
	if (result->HasError()) {
		std::ostringstream error_msg;
		error_msg << "-------------------------------------\n";
		error_msg << "Arrow round-trip type comparison failed\n";
		error_msg << "-------------------------------------\n";
		error_msg << "Query: " << query.c_str() << "\n";
		error_msg << "-------------------------------------\n";
		error_msg << "Query failed to execute:\n";
		error_msg << result->GetError();
		error_msg << "-------------------------------------\n";
		printf("%s", error_msg.str().c_str());
		return false;
	}
	vector<string> rows;
	for (auto &row : *result) {
		string row_str;
		for (idx_t c = 0; c < result->ColumnCount(); ++c) {
			if (!row_str.empty()) {
				row_str += "\t";
			}
			// render through the Value so a NULL in a differing row prints instead of throwing
			row_str += row.GetBaseValue(c).ToString();
		}
		rows.push_back(row_str);
	}
	if (!rows.empty()) {
		std::ostringstream error_msg;
		error_msg << "-------------------------------------\n";
		error_msg << "Arrow round-trip type comparison failed\n";
		error_msg << "-------------------------------------\n";
		error_msg << "Query: " << query.c_str() << "\n";
		error_msg << "-------------------------------------\n";
		error_msg << "Rows existed in Arrow result set but not in regular result set:\n";
		error_msg << "-------------------------------------\n";
		for (auto &row : rows) {
			error_msg << row << "\n";
		}
		printf("%s", error_msg.str().c_str());
		return false;
	}
	return true;
}

class BorrowedArrowScanFactory : public ArrowScanFactory {
public:
	explicit BorrowedArrowScanFactory(ArrowScanFactory &factory_p) : factory(factory_p) {
	}
	void GetSchema(ArrowSchema &schema) override {
		factory.get().GetSchema(schema);
	}
	unique_ptr<ArrowArrayStreamWrapper> ProduceStream(ArrowStreamParameters &parameters) override {
		return factory.get().ProduceStream(parameters);
	}

private:
	reference<ArrowScanFactory> factory;
};

shared_ptr<ArrowScanFactory> ArrowTestHelper::ConstructArrowScan(ArrowTestFactory &factory) {
	return make_shared_ptr<BorrowedArrowScanFactory>(factory);
}

shared_ptr<ArrowScanFactory> ArrowTestHelper::ConstructArrowScan(ArrowArrayStream &stream) {
	return make_shared_ptr<ArrowStreamTestFactory>(stream);
}

bool ArrowTestHelper::RunArrowComparison(Connection &con, const string &query, bool big_result) {
	// we can't have a too large number here because a multiple of this batch size is passed into an allocation
	idx_t batch_size = big_result ? 1000000 : 10000;
	auto initial_result = con.context->Query(query, make_shared_ptr<ArrowFormat>(batch_size));
	if (initial_result->HasError()) {
		initial_result->Print();
		printf("Query: %s\n", query.c_str());
		return false;
	}

	auto client_properties = con.context->GetClientProperties();
	auto types = initial_result->GetTypes();
	auto names = duckdb::IdentifiersToStrings(initial_result->GetNames());
	// We create an "arrow object" that consists of the Arrow arrays the query produced
	ArrowTestFactory factory(std::move(types), std::move(names), std::move(initial_result), client_properties,
	                         *con.context);
	// And construct a `arrow_scan` to read the created "arrow object"
	auto params = ConstructArrowScan(factory);

	return CompareResults(con, std::move(params), query);
}

bool ArrowTestHelper::RunArrowComparison(Connection &con, const string &query, ArrowArrayStream &arrow_stream) {
	if (!arrow_stream.private_data) {
		// no data - skip comparison
		return true;
	}
	// construct the arrow scan
	auto params = ConstructArrowScan(arrow_stream);

	auto success = CompareResults(con, std::move(params), query);
	arrow_stream.release = nullptr;
	return success;
}

} // namespace duckdb
