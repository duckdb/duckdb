#include "duckdb/main/result_format.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/buffered_data/buffered_data.hpp"

namespace duckdb {

ResultFormatGlobalState::~ResultFormatGlobalState() {
}

ResultFormatLocalState::~ResultFormatLocalState() {
}

ResultFormat::~ResultFormat() {
}

bool ResultFormat::IsChunk() const {
	return StringUtil::Equals(Name(), ChunkFormat::NAME);
}

const shared_ptr<ResultFormat> &ResultFormat::Chunk() {
	static const shared_ptr<ResultFormat> chunk_format = make_shared_ptr<ChunkFormat>();
	return chunk_format;
}

//===--------------------------------------------------------------------===//
// ChunkFormat
//===--------------------------------------------------------------------===//
class ChunkFormatLocalState : public ResultFormatLocalState {
public:
	unique_ptr<ChunkUnit> unit;
};

const char *ChunkFormat::Name() const {
	return NAME;
}

unique_ptr<ResultFormatGlobalState> ChunkFormat::InitGlobal(const vector<LogicalType> &types,
                                                            const vector<Identifier> &names,
                                                            const ClientProperties &properties,
                                                            ResultOrdering ordering) {
	return make_uniq<ResultFormatGlobalState>();
}

unique_ptr<ResultFormatLocalState> ChunkFormat::InitLocal(ResultFormatGlobalState &gstate) {
	return make_uniq<ChunkFormatLocalState>();
}

void ChunkFormat::Append(ResultFormatGlobalState &gstate, ResultFormatLocalState &lstate_p, DataChunk &chunk) {
	auto &lstate = lstate_p.Cast<ChunkFormatLocalState>();
	D_ASSERT(!lstate.unit);
	// Copied outside the buffer's lock, so parallel producers copy concurrently
	lstate.unit = make_uniq<ChunkUnit>(BufferedData::CopyForBuffering(chunk));
}

bool ChunkFormat::IsFull(ResultFormatLocalState &lstate) {
	return true;
}

unique_ptr<ResultUnit> ChunkFormat::Finish(ResultFormatGlobalState &gstate, ResultFormatLocalState &lstate_p) {
	auto &lstate = lstate_p.Cast<ChunkFormatLocalState>();
	return std::move(lstate.unit);
}

} // namespace duckdb
