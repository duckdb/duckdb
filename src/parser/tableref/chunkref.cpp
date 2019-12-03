#include "duckdb/parser/tableref/chunkref.hpp"

#include "duckdb/common/serializer.hpp"

using namespace duckdb;
using namespace std;

bool ChunkRef::Equals(const TableRef *other_) const {
	if (!TableRef::Equals(other_)) {
		return false;
	}
	auto other = (ChunkRef *)other_;
	throw NotImplementedException("FIXME: compare ChunkRef");
}

void ChunkRef::Serialize(Serializer &serializer) {
	TableRef::Serialize(serializer);
	chunk.Serialize(serializer);
}

unique_ptr<TableRef> ChunkRef::Deserialize(Deserializer &source) {
	auto result = make_unique<ChunkRef>();
	result->chunk.Deserialize(source);
	return move(result);
}

unique_ptr<TableRef> ChunkRef::Copy() {
	auto copy = make_unique<ChunkRef>();
	chunk.Copy(copy->chunk);
	copy->alias = alias;
	return move(copy);
}
