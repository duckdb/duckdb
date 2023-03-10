#include "macaddr_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include <iostream>

namespace duckdb {

using MACADDR_TYPE = StructTypeTernary<uint16_t, uint16_t, uint16_t>;

bool MACAddrFunctions::CastVarcharToMAC(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto constant = source.GetVectorType() == VectorType::CONSTANT_VECTOR;

	UnifiedVectorFormat vdata;
	source.ToUnifiedFormat(count, vdata);

	auto &entries = StructVector::GetEntries(result);
	auto a = FlatVector::GetData<uint16_t>(*entries[0]);
	auto b = FlatVector::GetData<uint16_t>(*entries[1]);
	auto c = FlatVector::GetData<uint16_t>(*entries[2]);

	auto input = (string_t *)vdata.data;
	bool success = true;
	for (idx_t i = 0; i < (constant ? 1 : count); i++) {
		auto idx = vdata.sel->get_index(i);

		if (!vdata.validity.RowIsValid(idx)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}
		MACAddr mac;
		if (!MACAddr::TryParse(input[idx], mac, parameters.error_message)) {
			FlatVector::SetNull(result, i, true);
			success = false;
			continue;
		}
		a[i] = uint16_t(mac.a);
		b[i] = uint16_t(mac.b);
		c[i] = uint16_t(mac.c);
	}
	if (constant) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	return success;
}

bool MACAddrFunctions::CastMACToVarchar(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	GenericExecutor::ExecuteUnary<MACADDR_TYPE, PrimitiveType<string_t>>(
	    source, result, count, [&](MACADDR_TYPE input) {
		    MACAddr mac(input.a_val, input.b_val, input.c_val);
		    auto str = mac.ToString();
		    return StringVector::AddString(result, str);
	    });
	return true;
}

} // namespace duckdb
