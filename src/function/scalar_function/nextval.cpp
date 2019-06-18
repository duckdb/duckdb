#include "function/scalar_function/nextval.hpp"

#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/sequence_catalog_entry.hpp"
#include "common/exception.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"
#include "main/client_context.hpp"
#include "main/database.hpp"
#include "planner/expression/bound_function_expression.hpp"

using namespace std;

namespace duckdb {

struct NextvalBindData : public FunctionData {
	//! The client context for the function call
	ClientContext &context;
	//! The sequence to use for the nextval computation; only if
	SequenceCatalogEntry *sequence;

	NextvalBindData(ClientContext &context, SequenceCatalogEntry *sequence) : context(context), sequence(sequence) {
	}

	unique_ptr<FunctionData> Copy() override {
		return make_unique<NextvalBindData>(context, sequence);
	}
};

static void parse_schema_and_sequence(string input, string &schema, string &name) {
	index_t start = 0;
	for (const char *istr = input.c_str(); *istr; istr++) {
		if (*istr == '.') {
			// separator
			index_t len = istr - input.c_str();
			if (len == 0) {
				throw ParserException("invalid name syntax");
			}
			if (start > 0) {
				throw ParserException("invalid name syntax");
			}
			schema = input.substr(0, len);
			start = len + 1;
		}
	}
	if (start == 0) {
		schema = DEFAULT_SCHEMA;
		name = input;
	} else {
		name = input.substr(start, input.size() - start);
	}
}

static int64_t next_sequence_value(Transaction &transaction, SequenceCatalogEntry *seq) {
	lock_guard<mutex> seqlock(seq->lock);
	int64_t result;
	if (seq->cycle) {
		result = seq->counter;
		seq->counter += seq->increment;
		;
		if (result < seq->min_value) {
			result = seq->max_value;
			seq->counter = seq->max_value + seq->increment;
		} else if (result > seq->max_value) {
			result = seq->min_value;
			seq->counter = seq->min_value + seq->increment;
		}
	} else {
		result = seq->counter;
		seq->counter += seq->increment;
		if (result < seq->min_value) {
			throw SequenceException("nextval: reached minimum value of sequence \"%s\" (%lld)", seq->name.c_str(),
			                        seq->min_value);
		}
		if (result > seq->max_value) {
			throw SequenceException("nextval: reached maximum value of sequence \"%s\" (%lld)", seq->name.c_str(),
			                        seq->max_value);
		}
	}
	seq->usage_count++;
	transaction.sequence_usage[seq] = SequenceValue(seq->usage_count, seq->counter);
	return result;
}

void nextval_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                      Vector &result) {
	auto &info = (NextvalBindData &)*expr.bind_info;
	assert(input_count == 1 && inputs[0].type == TypeId::VARCHAR);
	result.Initialize(TypeId::BIGINT);

	if (exec.chunk) {
		result.count = exec.chunk->size();
		result.sel_vector = exec.chunk->sel_vector;
	} else {
		result.count = inputs[0].count;
		result.sel_vector = inputs[0].sel_vector;
	}
	Transaction &transaction = info.context.ActiveTransaction();
	if (info.sequence) {
		// sequence to use is hard coded
		// increment the sequence
		int64_t *result_data = (int64_t *)result.data;
		VectorOperations::Exec(result, [&](index_t i, index_t k) {
			// get the next value from the sequence
			result_data[i] = next_sequence_value(transaction, info.sequence);
		});
	} else {
		// sequence to use comes from the input
		assert(result.count == inputs[0].count && result.sel_vector == inputs[0].sel_vector);
		int64_t *result_data = (int64_t *)result.data;
		VectorOperations::ExecType<const char *>(inputs[0], [&](const char *value, index_t i, index_t k) {
			// first get the sequence schema/name
			string schema, seq;
			string seqname = string(value);
			parse_schema_and_sequence(seqname, schema, seq);
			// fetch the sequence from the catalog
			auto sequence = info.context.catalog.GetSequence(info.context.ActiveTransaction(), schema, seq);
			// finally get the next value from the sequence
			result_data[i] = next_sequence_value(transaction, sequence);
		});
	}
}

bool nextval_matches_arguments(vector<SQLType> &arguments) {
	if (arguments.size() != 1) {
		return false;
	}
	return arguments[0].id == SQLTypeId::VARCHAR;
}

SQLType nextval_get_return_type(vector<SQLType> &arguments) {
	return SQLType(SQLTypeId::BIGINT);
}

unique_ptr<FunctionData> nextval_bind(BoundFunctionExpression &expr, ClientContext &context) {
	SequenceCatalogEntry *sequence = nullptr;
	if (expr.children[0]->IsFoldable()) {
		string schema, seq;
		// parameter to nextval function is a foldable constant
		// evaluate the constant and perform the catalog lookup already
		Value seqname = ExpressionExecutor::EvaluateScalar(*expr.children[0]);
		assert(seqname.type == TypeId::VARCHAR);
		parse_schema_and_sequence(seqname.str_value, schema, seq);
		sequence = context.catalog.GetSequence(context.ActiveTransaction(), schema, seq);
	}
	return make_unique<NextvalBindData>(context, sequence);
}

void nextval_dependency(BoundFunctionExpression &expr, unordered_set<CatalogEntry *> &dependencies) {
	auto &info = (NextvalBindData &)*expr.bind_info;
	if (info.sequence) {
		dependencies.insert(info.sequence);
	}
}

} // namespace duckdb
