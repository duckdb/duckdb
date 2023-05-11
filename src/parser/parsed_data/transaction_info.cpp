#include "duckdb/parser/parsed_data/transaction_info.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

TransactionInfo::TransactionInfo(TransactionType type) : type(type) {
}

void TransactionInfo::Serialize(Serializer &serializer) const {
	FieldWriter writer(serializer);
	writer.WriteField(type);
	writer.Finalize();
}

unique_ptr<ParseInfo> TransactionInfo::Deserialize(Deserializer &deserializer) {
	FieldReader reader(deserializer);
	auto transaction_type = reader.ReadRequired<TransactionType>();
	reader.Finalize();

	auto transaction_info = make_uniq<TransactionInfo>(transaction_type);
	return std::move(transaction_info);
}

} // namespace duckdb
