#include "duckdb.hpp"
#include "duckdb_node.hpp"
#include "napi.h"

#include <thread>

namespace node_duckdb {

Napi::Array EncodeDataChunk(Napi::Env env, duckdb::DataChunk &chunk, bool with_types, bool with_data) {
	Napi::Array col_descs(Napi::Array::New(env, chunk.ColumnCount()));
	for (idx_t col_idx = 0; col_idx < chunk.ColumnCount(); col_idx++) {
		auto col_desc = Napi::Object::New(env);

		// Make sure we only have flat vectors hereafter (for now)
		auto &chunk_vec = chunk.data[col_idx];
		if (with_data) {
			chunk_vec.Flatten(chunk.size());
		}

		// Do a post-order DFS traversal
		vector<std::tuple<bool, duckdb::Vector *, Napi::Object, size_t, size_t>> pending;
		pending.emplace_back(false, &chunk_vec, Napi::Object::New(env), 0, 0);

		while (!pending.empty()) {
			// Unpack DFS node
			auto &back = pending.back();
			auto &visited = std::get<0>(back);
			auto &vec = std::get<1>(back);
			auto &desc = std::get<2>(back);
			auto &parent_idx = std::get<3>(back);
			auto &idx_in_parent = std::get<4>(back);

			// Already visited?
			if (visited) {
				if (pending.size() == 1) {
					col_desc = desc;
					break;
				}
				std::get<2>(pending[parent_idx]).Get("children").As<Napi::Array>().Set(idx_in_parent, desc);
				pending.pop_back();
				continue;
			}
			visited = true;
			auto current_idx = pending.size() - 1;

			// Store types
			auto &vec_type = vec->GetType();
			if (with_types) {
				desc.Set("sqlType", vec_type.ToString());
				desc.Set("physicalType", TypeIdToString(vec_type.InternalType()));
			}

			// Create validity vector
			if (with_data) {
				vec->Flatten(chunk.size());
				auto &validity = duckdb::FlatVector::Validity(*vec);
				auto validity_buffer = Napi::Uint8Array::New(env, chunk.size());
				for (idx_t row_idx = 0; row_idx < chunk.size(); row_idx++) {
					validity_buffer[row_idx] = validity.RowIsValid(row_idx);
				}
				desc.Set("validity", validity_buffer);
			}

			// Create data buffer
			switch (vec_type.id()) {
			case duckdb::LogicalTypeId::TINYINT: {
				if (with_data) {
					auto array = Napi::Int8Array::New(env, chunk.size());
					auto data = duckdb::FlatVector::GetData<int8_t>(*vec);
					for (size_t i = 0; i < chunk.size(); ++i) {
						array[i] = data[i];
					}
					desc.Set("data", array);
				}
				break;
			}
			case duckdb::LogicalTypeId::SMALLINT: {
				if (with_data) {
					auto array = Napi::Int16Array::New(env, chunk.size());
					auto data = duckdb::FlatVector::GetData<int16_t>(*vec);
					for (size_t i = 0; i < chunk.size(); ++i) {
						array[i] = data[i];
					}
					desc.Set("data", array);
				}
				break;
			}
			case duckdb::LogicalTypeId::INTEGER: {
				if (with_data) {
					auto array = Napi::Int32Array::New(env, chunk.size());
					auto data = duckdb::FlatVector::GetData<int32_t>(*vec);
					for (size_t i = 0; i < chunk.size(); ++i) {
						array[i] = data[i];
					}
					desc.Set("data", array);
				}
				break;
			}
			case duckdb::LogicalTypeId::DOUBLE: {
				if (with_data) {
					auto array = Napi::Float64Array::New(env, chunk.size());
					auto data = duckdb::FlatVector::GetData<double>(*vec);
					for (size_t i = 0; i < chunk.size(); ++i) {
						array[i] = data[i];
					}
					desc.Set("data", array);
				}
				break;
			}
			case duckdb::LogicalTypeId::BIGINT:
			case duckdb::LogicalTypeId::TIME:
			case duckdb::LogicalTypeId::TIME_TZ:
			case duckdb::LogicalTypeId::TIMESTAMP_MS:
			case duckdb::LogicalTypeId::TIMESTAMP_NS:
			case duckdb::LogicalTypeId::TIMESTAMP_SEC:
			case duckdb::LogicalTypeId::TIMESTAMP: {
				if (with_data) {
#if NAPI_VERSION > 5
					auto array = Napi::BigInt64Array::New(env, chunk.size());
					auto data = duckdb::FlatVector::GetData<int64_t>(*vec);
#else
					auto array = Napi::Float64Array::New(env, chunk.size());
					auto data = duckdb::FlatVector::GetData<int64_t>(*vec);
#endif
					for (size_t i = 0; i < chunk.size(); ++i) {
						array[i] = data[i];
					}
					desc.Set("data", array);
				}
				break;
			}
			case duckdb::LogicalTypeId::UBIGINT: {
				if (with_data) {
#if NAPI_VERSION > 5
					auto array = Napi::BigUint64Array::New(env, chunk.size());
					auto data = duckdb::FlatVector::GetData<uint64_t>(*vec);
#else
					auto array = Napi::Float64Array::New(env, chunk.size());
					auto data = duckdb::FlatVector::GetData<int64_t>(*vec);
#endif
					for (size_t i = 0; i < chunk.size(); ++i) {
						array[i] = data[i];
					}
					desc.Set("data", array);
				}
				break;
			}
			case duckdb::LogicalTypeId::BLOB: {
				if (with_data) {
					auto array = Napi::Array::New(env, chunk.size());
					auto data = duckdb::FlatVector::GetData<duckdb::string_t>(*vec);

					for (size_t i = 0; i < chunk.size(); ++i) {
						auto buf = Napi::Buffer<char>::Copy(env, data[i].GetData(), data[i].GetSize());
						array.Set(i, buf);
					}
					desc.Set("data", array);
				}
				break;
			}
			case duckdb::LogicalTypeId::VARCHAR: {
				if (with_data) {
					auto array = Napi::Array::New(env, chunk.size());
					auto data = duckdb::FlatVector::GetData<duckdb::string_t>(*vec);
					for (size_t i = 0; i < chunk.size(); ++i) {
						array.Set(i, data[i].GetString());
					}
					desc.Set("data", array);
				}
				break;
			}
			case duckdb::LogicalTypeId::STRUCT: {
				auto child_count = duckdb::StructType::GetChildCount(vec_type);
				auto &entries = duckdb::StructVector::GetEntries(*vec);
				desc.Set("children", Napi::Array::New(env, child_count));
				for (size_t i = 0; i < child_count; ++i) {
					auto c = child_count - 1 - i;
					auto &entry = entries[c];
					auto desc = Napi::Object::New(env);
					auto name = duckdb::StructType::GetChildName(vec_type, c);
					desc.Set("name", name);
					pending.emplace_back(false, entry.get(), desc, current_idx, i);
				}
				break;
			}
			default:
				Napi::TypeError::New(env, "Unsupported UDF argument type " + vec->GetType().ToString())
				    .ThrowAsJavaScriptException();
				break;
			}
		}
		col_descs.Set(col_idx, col_desc);
	}
	return col_descs;
}

} // namespace node_duckdb
