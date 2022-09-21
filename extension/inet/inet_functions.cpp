#include "inet_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/vector_operations/struct_executor.hpp"

namespace duckdb {

bool INetFunctions::CastVarcharToINET(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto constant = source.GetVectorType() == VectorType::CONSTANT_VECTOR;

	UnifiedVectorFormat vdata;
	source.ToUnifiedFormat(count, vdata);

	auto &entries = StructVector::GetEntries(result);
	auto address_data = FlatVector::GetData<hugeint_t>(*entries[0]);
	auto mask_data = FlatVector::GetData<uint16_t>(*entries[1]);
	auto ip_type = FlatVector::GetData<uint8_t>(*entries[2]);

	auto input = (string_t *)vdata.data;
	bool success = true;
	for (idx_t i = 0; i < (constant ? 1 : count); i++) {
		auto idx = vdata.sel->get_index(i);

		if (!vdata.validity.RowIsValid(idx)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}
		IPAddress inet;
		if (!IPAddress::TryParse(input[idx], inet, parameters.error_message)) {
			FlatVector::SetNull(result, i, true);
			success = false;
			continue;
		}
		address_data[i] = inet.address;
		mask_data[i] = inet.mask;
		ip_type[i] = uint8_t(inet.type);
	}
	if (constant) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	return success;
}

bool INetFunctions::CastINETToVarchar(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	StructExecutor::Execute<hugeint_t, uint16_t, uint8_t, string_t>(
	    source, result, count, [&](hugeint_t address, uint16_t mask, uint8_t ip_type) {
		    IPAddress inet(address, mask, IPAddressType(ip_type));
		    auto str = inet.ToString();
		    return StringVector::AddString(result, str);
	    });
	return true;
}

void INetFunctions::Host(DataChunk &args, ExpressionState &state, Vector &result) {
	StructExecutor::Execute<hugeint_t, uint16_t, uint8_t, string_t>(
	    args.data[0], result, args.size(), [&](hugeint_t address, uint16_t mask, uint8_t ip_type) {
		    IPAddress inet(address, 32, IPAddressType(ip_type));
		    auto str = inet.ToString();
		    return StringVector::AddString(result, str);
	    });
}

} // namespace duckdb
