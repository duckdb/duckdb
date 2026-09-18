#include "duckdb/common/arrow/arrow_format.hpp"

#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/deque.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"

namespace duckdb {

ArrowUnit::ArrowUnit(idx_t row_count, idx_t byte_size) : ResultUnit(row_count, byte_size) {
}

ArrowFormatGlobalState::ArrowFormatGlobalState(vector<LogicalType> types_p, const vector<Identifier> &names,
                                               const ClientProperties &properties_p)
    : types(std::move(types_p)), properties(properties_p) {
	if (!properties.client_context) {
		throw InternalException("The Arrow format needs the client context of the query that settled it");
	}
	extension_types = ArrowTypeExtensionData::GetExtensionTypes(*properties.client_context, types);
	ArrowConverter::ToArrowSchema(&schema.arrow_schema, types, IdentifiersToStrings(names), properties);
}

ArrowFormatGlobalState::~ArrowFormatGlobalState() {
}

//===--------------------------------------------------------------------===//
// ArrowFormat
//===--------------------------------------------------------------------===//
namespace {

class ArrowFormatLocalState : public ResultFormatLocalState {
public:
	//! The array being built. Created at the first row that goes into it
	unique_ptr<ArrowAppender> appender;
	//! Arrays that reached the batch size and have not been handed over yet
	deque<unique_ptr<ArrowUnit>> sealed;
};

unique_ptr<ArrowUnit> SealAppender(ArrowFormatLocalState &lstate) {
	// Finalize hands the buffers to the array, so the size has to be read before it
	auto unit = make_uniq<ArrowUnit>(lstate.appender->RowCount(), lstate.appender->ByteSize());
	unit->array.arrow_array = lstate.appender->Finalize();
	lstate.appender.reset();
	return unit;
}

} // namespace

ArrowFormat::ArrowFormat(idx_t batch_size_p) : batch_size(batch_size_p) {
	if (batch_size == 0) {
		throw InvalidInputException("The Arrow format needs a batch size of at least one row");
	}
}

const char *ArrowFormat::Name() const {
	return NAME;
}

unique_ptr<ResultFormatGlobalState> ArrowFormat::InitGlobal(const vector<LogicalType> &types,
                                                            const vector<Identifier> &names,
                                                            const ClientProperties &properties,
                                                            ResultOrdering ordering) {
	return make_uniq<ArrowFormatGlobalState>(types, names, properties);
}

unique_ptr<ResultFormatLocalState> ArrowFormat::InitLocal(ResultFormatGlobalState &gstate) {
	return make_uniq<ArrowFormatLocalState>();
}

void ArrowFormat::Append(ResultFormatGlobalState &gstate_p, ResultFormatLocalState &lstate_p, DataChunk &chunk) {
	auto &gstate = gstate_p.Cast<ArrowFormatGlobalState>();
	auto &lstate = lstate_p.Cast<ArrowFormatLocalState>();
	auto count = chunk.size();
	idx_t processed = 0;
	while (processed < count) {
		if (!lstate.appender) {
			auto capacity = MinValue(batch_size, count - processed);
			lstate.appender =
			    make_uniq<ArrowAppender>(gstate.Types(), capacity, gstate.Properties(), gstate.ExtensionTypes());
		}
		auto to_append = MinValue(batch_size - lstate.appender->RowCount(), count - processed);
		lstate.appender->Append(chunk, processed, processed + to_append, count);
		processed += to_append;
		if (lstate.appender->RowCount() >= batch_size) {
			lstate.sealed.push_back(SealAppender(lstate));
		}
	}
}

unique_ptr<ResultUnit> ArrowFormat::Finish(ResultFormatGlobalState &gstate, ResultFormatLocalState &lstate_p,
                                           bool flush_partial) {
	auto &lstate = lstate_p.Cast<ArrowFormatLocalState>();
	if (!lstate.sealed.empty()) {
		auto unit = std::move(lstate.sealed.front());
		lstate.sealed.pop_front();
		return std::move(unit);
	}
	if (!flush_partial || !lstate.appender || lstate.appender->RowCount() == 0) {
		return nullptr;
	}
	return SealAppender(lstate);
}

} // namespace duckdb
