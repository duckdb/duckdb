#include "duckdb/common/arrow/arrow_format.hpp"

#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/deque.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// ArrowPayload
//===--------------------------------------------------------------------===//
namespace {

//! One node of a view: its own struct tree over the owner's buffers, so a consumer that moves a
//! child out of this view, as the C interface allows, touches nothing another view still reads
struct SharedArrayView {
	shared_ptr<ArrowArrayWrapper> owner;
	vector<unique_ptr<ArrowArray>> children;
	vector<ArrowArray *> child_pointers;
	unique_ptr<ArrowArray> dictionary;
};

void ReleaseSharedArrayView(ArrowArray *array) {
	if (!array || !array->release) {
		return;
	}
	auto view = static_cast<SharedArrayView *>(array->private_data);
	for (auto &child : view->children) {
		// A child without a release was moved out and is released by whoever took it
		if (child->release) {
			child->release(child.get());
		}
	}
	if (view->dictionary && view->dictionary->release) {
		view->dictionary->release(view->dictionary.get());
	}
	delete view;
	array->private_data = nullptr;
	array->release = nullptr;
}

ArrowArray ViewOf(const ArrowArray &source, const shared_ptr<ArrowArrayWrapper> &owner) {
	auto view = make_uniq<SharedArrayView>();
	view->owner = owner;
	ArrowArray result = source;
	for (int64_t i = 0; i < source.n_children; i++) {
		view->children.push_back(make_uniq<ArrowArray>(ViewOf(*source.children[i], owner)));
		view->child_pointers.push_back(view->children.back().get());
	}
	result.children = view->child_pointers.data();
	if (source.dictionary) {
		view->dictionary = make_uniq<ArrowArray>(ViewOf(*source.dictionary, owner));
		result.dictionary = view->dictionary.get();
	}
	result.private_data = view.release();
	result.release = ReleaseSharedArrayView;
	return result;
}

ArrowArrayWrapper ViewOf(const shared_ptr<ArrowArrayWrapper> &owner) {
	ArrowArrayWrapper view;
	view.arrow_array = ViewOf(owner->arrow_array, owner);
	return view;
}

shared_ptr<ArrowArrayWrapper> Own(ArrowArray array) {
	auto owner = make_shared_ptr<ArrowArrayWrapper>();
	owner->arrow_array = array;
	return owner;
}

} // namespace

ArrowPayload::ArrowPayload(ArrowArray array_p) : ArrowPayload(Own(array_p)) {
}

ArrowPayload::ArrowPayload(shared_ptr<ArrowArrayWrapper> owner_p) : array(ViewOf(owner_p)), owner(std::move(owner_p)) {
}

unique_ptr<ArrowPayload> ArrowPayload::Copy() const {
	return make_uniq<ArrowPayload>(owner);
}

ArrowFormatGlobalState::ArrowFormatGlobalState(const ResultFormatContext &context)
    : types(context.types), properties(context.client_properties) {
	if (!properties.client_context) {
		throw InternalException("The Arrow format needs the client context of the query that submitted it");
	}
	extension_types = ArrowTypeExtensionData::GetExtensionTypes(*properties.client_context, types);
	ArrowConverter::ToArrowSchema(&schema.arrow_schema, types, IdentifiersToStrings(context.names), properties);
}

ArrowFormatGlobalState::~ArrowFormatGlobalState() {
}

//===--------------------------------------------------------------------===//
// ArrowFormat
//===--------------------------------------------------------------------===//
namespace {

class ArrowUnit : public ResultUnit {
public:
	ArrowUnit(idx_t row_count, idx_t byte_size, unique_ptr<ArrowPayload> payload_p)
	    : ResultUnit(row_count, byte_size), payload(std::move(payload_p)) {
	}

public:
	unique_ptr<ArrowPayload> payload;
};

class ArrowFormatLocalState : public ResultFormatLocalState {
public:
	//! The array being built. Created at the first row that goes into it
	unique_ptr<ArrowAppender> appender;
	//! Arrays that reached the batch size and have not been handed over yet
	deque<unique_ptr<ArrowUnit>> sealed;
};

unique_ptr<ArrowUnit> SealAppender(ArrowFormatLocalState &lstate) {
	// Finalize hands the buffers to the array, so the size has to be read before it
	auto rows = lstate.appender->RowCount();
	auto bytes = lstate.appender->ByteSize();
	auto unit = make_uniq<ArrowUnit>(rows, bytes, make_uniq<ArrowPayload>(lstate.appender->Finalize()));
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

unique_ptr<ResultFormatGlobalState> ArrowFormat::InitGlobal(const ResultFormatContext &context) {
	return make_uniq<ArrowFormatGlobalState>(context);
}

unique_ptr<ResultFormatLocalState> ArrowFormat::InitLocal(ResultFormatGlobalState &gstate) {
	return make_uniq<ArrowFormatLocalState>();
}

void ArrowFormat::AppendToUnit(ResultFormatGlobalState &gstate_p, ResultFormatLocalState &lstate_p, DataChunk &chunk) {
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

bool ArrowFormat::IsUnitFinished(ResultFormatLocalState &lstate) {
	return !lstate.Cast<ArrowFormatLocalState>().sealed.empty();
}

unique_ptr<ResultUnit> ArrowFormat::FinishUnit(ResultFormatGlobalState &gstate, ResultFormatLocalState &lstate_p) {
	auto &lstate = lstate_p.Cast<ArrowFormatLocalState>();
	if (!lstate.sealed.empty()) {
		auto unit = std::move(lstate.sealed.front());
		lstate.sealed.pop_front();
		return std::move(unit);
	}
	if (!lstate.appender || lstate.appender->RowCount() == 0) {
		return nullptr;
	}
	return SealAppender(lstate);
}

unique_ptr<ArrowPayload> ArrowFormat::UnpackUnit(unique_ptr<ResultUnit> unit) {
	if (!unit) {
		return nullptr;
	}
	return std::move(unit->Cast<ArrowUnit>().payload);
}

unique_ptr<ArrowPayload> ArrowFormat::CopyPayload(const ArrowPayload &payload) {
	return payload.Copy();
}

} // namespace duckdb
