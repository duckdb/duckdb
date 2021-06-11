#include "duckdb/common/arrow_wrapper.hpp"
#include "duckdb.hpp"

#include "duckdb/common/arrow.hpp"
#include "duckdb/function/table/arrow.hpp"

#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/to_string.hpp"
#include "utf8proc_wrapper.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/limits.hpp"

namespace duckdb {

LogicalType GetArrowLogicalType(ArrowSchema &schema,
                                std::unordered_map<idx_t, unique_ptr<ArrowConvertData>> &arrow_convert_data,
                                idx_t col_idx) {
	auto format = string(schema.format);
	if (format == "+l" || format == "+L" || (format[0] == '+' && format[1] == 'w')) {
		//! This is a list, we might have to create a list_convert_data in our arrow_convert_data
		if (arrow_convert_data.find(col_idx) == arrow_convert_data.end()) {
			arrow_convert_data[col_idx] = make_unique<ListArrowConvertData>();
		}
	}
	if (format == "n") {
		return LogicalType::SQLNULL;
	} else if (format == "b") {
		return LogicalType::BOOLEAN;
	} else if (format == "c") {
		return LogicalType::TINYINT;
	} else if (format == "s") {
		return LogicalType::SMALLINT;
	} else if (format == "i") {
		return LogicalType::INTEGER;
	} else if (format == "l") {
		return LogicalType::BIGINT;
	} else if (format == "C") {
		return LogicalType::UTINYINT;
	} else if (format == "S") {
		return LogicalType::USMALLINT;
	} else if (format == "I") {
		return LogicalType::UINTEGER;
	} else if (format == "L") {
		return LogicalType::UBIGINT;
	} else if (format == "f") {
		return LogicalType::FLOAT;
	} else if (format == "g") {
		return LogicalType::DOUBLE;
	} else if (format[0] == 'd') { //! this can be either decimal128 or decimal 256 (e.g., d:38,0)
		std::string parameters = format.substr(format.find(':'));
		uint8_t width = std::stoi(parameters.substr(1, parameters.find(',')));
		uint8_t scale = std::stoi(parameters.substr(parameters.find(',') + 1));
		if (width > 38) {
			throw NotImplementedException("Unsupported Internal Arrow Type for Decimal %s", format);
		}
		return LogicalType::DECIMAL(width, scale);
	} else if (format == "u") {
		return LogicalType::VARCHAR;
	} else if (format == "tsn:") {
		return LogicalTypeId::TIMESTAMP_NS;
	} else if (format == "tsu:") {
		return LogicalTypeId::TIMESTAMP;
	} else if (format == "tsm:") {
		return LogicalTypeId::TIMESTAMP_MS;
	} else if (format == "tss:") {
		return LogicalTypeId::TIMESTAMP_SEC;
	} else if (format == "tdD") {
		return LogicalType::DATE;
	} else if (format == "ttm") {
		return LogicalType::TIME;
	} else if (format == "+l") {

		((ListArrowConvertData *)arrow_convert_data[col_idx].get())->list_type.emplace_back(ArrowListType::NORMAL, 0);
		auto child_type = GetArrowLogicalType(*schema.children[0], arrow_convert_data, col_idx);
		return LogicalType::LIST(child_type);
	} else if (format == "+L") {
		((ListArrowConvertData *)arrow_convert_data[col_idx].get())
		    ->list_type.emplace_back(ArrowListType::SUPER_SIZE, 0);
		auto child_type = GetArrowLogicalType(*schema.children[0], arrow_convert_data, col_idx);
		return LogicalType::LIST(child_type);
	} else if (format[0] == '+' && format[1] == 'w') {
		std::string parameters = format.substr(format.find(':') + 1);
		idx_t fixed_size = std::stoi(parameters);
		((ListArrowConvertData *)arrow_convert_data[col_idx].get())
		    ->list_type.emplace_back(ArrowListType::FIXED_SIZE, fixed_size);
		auto child_type = GetArrowLogicalType(*schema.children[0], arrow_convert_data, col_idx);
		return LogicalType::LIST(move(child_type));
	} else if (format == "+s") {
		child_list_t<LogicalType> child_types;
		for (idx_t type_idx = 0; type_idx < (idx_t)schema.n_children; type_idx++) {
			auto child_type = GetArrowLogicalType(*schema.children[type_idx], arrow_convert_data, col_idx);
			child_types.push_back({schema.children[type_idx]->name, child_type});
		}
		return LogicalType::STRUCT(move(child_types));

	} else if (format == "+m") {
		child_list_t<LogicalType> child_types;
		//! First type will be struct, so we skip it
		auto &struct_schema = *schema.children[0];
		for (idx_t type_idx = 0; type_idx < (idx_t)struct_schema.n_children; type_idx++) {
			//! The other types must be added on lists
			auto child_type = GetArrowLogicalType(*struct_schema.children[type_idx], arrow_convert_data, col_idx);

			auto list_type = LogicalType::LIST(child_type);
			child_types.push_back({struct_schema.children[type_idx]->name, list_type});
		}
		return LogicalType::MAP(move(child_types));
	} else {
		throw NotImplementedException("Unsupported Internal Arrow Type %s", format);
	}
}

unique_ptr<FunctionData> ArrowTableFunction::ArrowScanBind(ClientContext &context, vector<Value> &inputs,
                                                           unordered_map<string, Value> &named_parameters,
                                                           vector<LogicalType> &input_table_types,
                                                           vector<string> &input_table_names,
                                                           vector<LogicalType> &return_types, vector<string> &names) {

	auto stream_factory_ptr = inputs[0].GetPointer();
	unique_ptr<ArrowArrayStreamWrapper> (*stream_factory_produce)(uintptr_t stream_factory_ptr) =
	    (unique_ptr<ArrowArrayStreamWrapper>(*)(uintptr_t stream_factory_ptr))inputs[1].GetPointer();
	auto rows_per_thread = inputs[2].GetValue<uint64_t>();

	auto res = make_unique<ArrowScanFunctionData>(rows_per_thread);
	auto &data = *res;
	data.stream = stream_factory_produce(stream_factory_ptr);
	if (!data.stream) {
		throw InvalidInputException("arrow_scan: NULL pointer passed");
	}

	data.stream->GetSchema(data.schema_root);

	for (idx_t col_idx = 0; col_idx < (idx_t)data.schema_root.arrow_schema.n_children; col_idx++) {
		auto &schema = *data.schema_root.arrow_schema.children[col_idx];
		if (!schema.release) {
			throw InvalidInputException("arrow_scan: released schema passed");
		}
		if (schema.dictionary) {
			res->arrow_convert_data[col_idx] =
			    make_unique<DictionaryArrowConvertData>(GetArrowLogicalType(schema, res->arrow_convert_data, col_idx));
			return_types.emplace_back(GetArrowLogicalType(*schema.dictionary, res->arrow_convert_data, col_idx));
		} else {
			return_types.emplace_back(GetArrowLogicalType(schema, res->arrow_convert_data, col_idx));
		}
		auto format = string(schema.format);
		auto name = string(schema.name);
		if (name.empty()) {
			name = string("v") + to_string(col_idx);
		}
		names.push_back(name);
	}
	return move(res);
}

unique_ptr<FunctionOperatorData> ArrowTableFunction::ArrowScanInit(ClientContext &context,
                                                                   const FunctionData *bind_data,
                                                                   const vector<column_t> &column_ids,
                                                                   TableFilterCollection *filters) {
	auto current_chunk = make_unique<ArrowArrayWrapper>();
	auto result = make_unique<ArrowScanState>(move(current_chunk));
	result->column_ids = column_ids;
	return move(result);
}

void SetValidityMask(Vector &vector, ArrowArray &array, ArrowScanState &scan_state, idx_t size, bool add_null = false) {
	auto &mask = FlatVector::Validity(vector);
	if (array.null_count != 0 && array.buffers[0]) {
		D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR);
		auto bit_offset = scan_state.chunk_offset + array.offset;
		auto n_bitmask_bytes = (size + 8 - 1) / 8;
		mask.EnsureWritable();
		if (bit_offset % 8 == 0) {
			//! just memcpy nullmask
			memcpy((void *)mask.GetData(), (uint8_t *)array.buffers[0] + bit_offset / 8, n_bitmask_bytes);
		} else {
			//! need to re-align nullmask
			bitset<STANDARD_VECTOR_SIZE + 8> temp_nullmask;
			memcpy(&temp_nullmask, (uint8_t *)array.buffers[0] + bit_offset / 8, n_bitmask_bytes + 1);

			temp_nullmask >>= (bit_offset % 8); //! why this has to be a right shift is a mystery to me
			memcpy((void *)mask.GetData(), (data_ptr_t)&temp_nullmask, n_bitmask_bytes);
		}
	}
	if (add_null) {
		//! We are setting a validity mask of the data part of dictionary vector
		//! For some reason, Nulls are allowed to be indexes, hence we need to set the last element here to be null
		//! We might have to resize the mask
		mask.Resize(size, size + 1);
		mask.SetInvalid(size);
	}
}

void GetValidityMask(ValidityMask &mask, ArrowArray &array, ArrowScanState &scan_state, idx_t size) {
	if (array.null_count != 0 && array.buffers[0]) {
		auto bit_offset = scan_state.chunk_offset + array.offset;
		auto n_bitmask_bytes = (size + 8 - 1) / 8;
		mask.EnsureWritable();
		if (bit_offset % 8 == 0) {
			//! just memcpy nullmask
			memcpy((void *)mask.GetData(), (uint8_t *)array.buffers[0] + bit_offset / 8, n_bitmask_bytes);
		} else {
			//! need to re-align nullmask
			bitset<STANDARD_VECTOR_SIZE + 8> temp_nullmask;
			memcpy(&temp_nullmask, (uint8_t *)array.buffers[0] + bit_offset / 8, n_bitmask_bytes + 1);

			temp_nullmask >>= (bit_offset % 8); //! why this has to be a right shift is a mystery to me
			memcpy((void *)mask.GetData(), (data_ptr_t)&temp_nullmask, n_bitmask_bytes);
		}
	}
}

void ColumnArrowToDuckDB(Vector &vector, ArrowArray &array, ArrowScanState &scan_state, idx_t size,
                         std::unordered_map<idx_t, unique_ptr<ArrowConvertData>> &arrow_convert_data, idx_t col_idx,
                         idx_t &list_col_idx);

void ArrowToDuckDBList(Vector &vector, ArrowArray &array, ArrowScanState &scan_state, idx_t size,
                       std::unordered_map<idx_t, unique_ptr<ArrowConvertData>> &arrow_convert_data, idx_t col_idx,
                       idx_t &list_col_idx) {
	auto original_type = ((ListArrowConvertData *)arrow_convert_data[col_idx].get())->list_type[list_col_idx++];
	idx_t list_size = 0;
	ListVector::Initialize(vector);
	auto &child_vector = ListVector::GetEntry(vector);
	SetValidityMask(vector, array, scan_state, size);

	if (original_type.first == ArrowListType::FIXED_SIZE) {
		//! Have to check validity mask before setting this up
		idx_t offset = 0;
		auto list_data = FlatVector::GetData<list_entry_t>(vector);
		for (idx_t i = 0; i < size; i++) {
			auto &le = list_data[i];
			le.offset = offset;
			le.length = original_type.second;
			offset += original_type.second;
		}
		list_size = offset;
	} else if (original_type.first == ArrowListType::NORMAL) {
		auto offsets = (uint32_t *)array.buffers[1] + array.offset + scan_state.chunk_offset;
		auto list_data = FlatVector::GetData<list_entry_t>(vector);
		for (idx_t i = 0; i < size; i++) {
			auto &le = list_data[i];
			le.offset = offsets[i];
			le.length = offsets[i + 1] - offsets[i];
		}
		list_size = offsets[size];
	} else {
		auto offsets = (uint64_t *)array.buffers[1] + array.offset + scan_state.chunk_offset;
		auto list_data = FlatVector::GetData<list_entry_t>(vector);
		for (idx_t i = 0; i < size; i++) {
			auto &le = list_data[i];
			le.offset = offsets[i];
			le.length = offsets[i + 1] - offsets[i];
		}
		list_size = offsets[size];
	}
	ListVector::SetListSize(vector, list_size);
	SetValidityMask(child_vector, *array.children[0], scan_state, list_size);
	ColumnArrowToDuckDB(child_vector, *array.children[0], scan_state, list_size, arrow_convert_data, col_idx,
	                    list_col_idx);
}
void ArrowToDuckDBMapList(Vector &vector, ArrowArray &array, ArrowScanState &scan_state, idx_t size,
                          std::unordered_map<idx_t, unique_ptr<ArrowConvertData>> &arrow_convert_data, idx_t col_idx,
                          idx_t &list_col_idx, uint32_t *offsets) {
	idx_t list_size = 0;
	ListVector::Initialize(vector);
	auto &child_vector = ListVector::GetEntry(vector);
	auto list_data = FlatVector::GetData<list_entry_t>(vector);
	for (idx_t i = 0; i < size; i++) {
		auto &le = list_data[i];
		le.offset = offsets[i];
		le.length = offsets[i + 1] - offsets[i];
	}
	list_size = offsets[size];

	ListVector::SetListSize(vector, list_size);
	SetValidityMask(child_vector, array, scan_state, list_size);
	ColumnArrowToDuckDB(child_vector, array, scan_state, list_size, arrow_convert_data, col_idx, list_col_idx);
}
void ColumnArrowToDuckDB(Vector &vector, ArrowArray &array, ArrowScanState &scan_state, idx_t size,
                         std::unordered_map<idx_t, unique_ptr<ArrowConvertData>> &arrow_convert_data, idx_t col_idx,
                         idx_t &list_col_idx) {
	switch (vector.GetType().id()) {
	case LogicalTypeId::SQLNULL:
		vector.Reference(Value());
		break;
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
		FlatVector::SetData(vector, (data_ptr_t)array.buffers[1] + GetTypeIdSize(vector.GetType().InternalType()) *
		                                                               (scan_state.chunk_offset + array.offset));
		break;

	case LogicalTypeId::VARCHAR: {
		auto offsets = (uint32_t *)array.buffers[1] + array.offset + scan_state.chunk_offset;
		auto cdata = (char *)array.buffers[2];

		for (idx_t row_idx = 0; row_idx < size; row_idx++) {
			if (FlatVector::IsNull(vector, row_idx)) {
				continue;
			}
			auto cptr = cdata + offsets[row_idx];
			auto str_len = offsets[row_idx + 1] - offsets[row_idx];

			auto utf_type = Utf8Proc::Analyze(cptr, str_len);
			if (utf_type == UnicodeType::INVALID) {
				throw std::runtime_error("Invalid UTF8 string encoding");
			}
			FlatVector::GetData<string_t>(vector)[row_idx] = StringVector::AddString(vector, cptr, str_len);
		}

		break;
	}
	case LogicalTypeId::TIME: {
		// convert time from milliseconds to microseconds
		auto src_ptr = (uint32_t *)array.buffers[1] + scan_state.chunk_offset + array.offset;
		auto tgt_ptr = (dtime_t *)FlatVector::GetData(vector);
		for (idx_t row = 0; row < size; row++) {
			tgt_ptr[row] = dtime_t(int64_t(src_ptr[row]) * 1000);
		}
		break;
	}
	case LogicalTypeId::DECIMAL: {
		auto val_mask = FlatVector::Validity(vector);
		//! We have to convert from INT128
		switch (vector.GetType().InternalType()) {
		case PhysicalType::INT16: {
			auto src_ptr = (hugeint_t *)array.buffers[1] + scan_state.chunk_offset + array.offset;
			auto tgt_ptr = (int16_t *)FlatVector::GetData(vector);
			for (idx_t row = 0; row < size; row++) {
				if (val_mask.RowIsValid(row)) {
					auto result = Hugeint::TryCast(src_ptr[row], tgt_ptr[row]);
					D_ASSERT(result);
				}
			}
			break;
		}
		case PhysicalType::INT32: {
			auto src_ptr = (hugeint_t *)array.buffers[1] + scan_state.chunk_offset + array.offset;
			auto tgt_ptr = (int32_t *)FlatVector::GetData(vector);
			for (idx_t row = 0; row < size; row++) {
				if (val_mask.RowIsValid(row)) {
					auto result = Hugeint::TryCast(src_ptr[row], tgt_ptr[row]);
					D_ASSERT(result);
				}
			}
			break;
		}
		case PhysicalType::INT64: {
			auto src_ptr = (hugeint_t *)array.buffers[1] + scan_state.chunk_offset + array.offset;
			auto tgt_ptr = (int64_t *)FlatVector::GetData(vector);
			for (idx_t row = 0; row < size; row++) {
				if (val_mask.RowIsValid(row)) {
					auto result = Hugeint::TryCast(src_ptr[row], tgt_ptr[row]);
					D_ASSERT(result);
				}
			}
			break;
		}
		case PhysicalType::INT128: {
			FlatVector::SetData(vector, (data_ptr_t)array.buffers[1] + GetTypeIdSize(vector.GetType().InternalType()) *
			                                                               (scan_state.chunk_offset + array.offset));
			break;
		}
		default:
			throw std::runtime_error("Unsupported physical type for Decimal" +
			                         TypeIdToString(vector.GetType().InternalType()));
		}
		break;
	}
	case LogicalTypeId::LIST: {
		ArrowToDuckDBList(vector, array, scan_state, size, arrow_convert_data, col_idx, list_col_idx);
		break;
	}
	case LogicalTypeId::MAP: {
		//! Since this is a map we skip first child, because its a struct
		auto &struct_arrow = *array.children[0];
		auto &child_entries = StructVector::GetEntries(vector);
		D_ASSERT(child_entries.size() == 2);
		auto offsets = (uint32_t *)array.buffers[1] + array.offset + scan_state.chunk_offset;
		//! Fill the children
		for (idx_t type_idx = 0; type_idx < (idx_t)struct_arrow.n_children; type_idx++) {
			ArrowToDuckDBMapList(*child_entries[type_idx], *struct_arrow.children[type_idx], scan_state, size,
			                     arrow_convert_data, col_idx, list_col_idx, offsets);
		}
		break;
	}
	case LogicalTypeId::STRUCT: {
		//! Fill the children
		auto &child_entries = StructVector::GetEntries(vector);
		for (idx_t type_idx = 0; type_idx < (idx_t)array.n_children; type_idx++) {
			SetValidityMask(*child_entries[type_idx], *array.children[type_idx], scan_state, size);
			ColumnArrowToDuckDB(*child_entries[type_idx], *array.children[type_idx], scan_state, size,
			                    arrow_convert_data, col_idx, list_col_idx);
		}
		break;
	}
	default:
		throw std::runtime_error("Unsupported type " + vector.GetType().ToString());
	}
}

template <class T>
static void SetSelectionVectorLoop(SelectionVector &sel, data_ptr_t indices_p, idx_t size) {
	auto indices = (T *)indices_p;
	for (idx_t row = 0; row < size; row++) {
		sel.set_index(row, indices[row]);
	}
}

template <class T>
static void SetSelectionVectorLoopWithChecks(SelectionVector &sel, data_ptr_t indices_p, idx_t size) {

	auto indices = (T *)indices_p;
	for (idx_t row = 0; row < size; row++) {
		if (indices[row] > NumericLimits<uint32_t>::Maximum()) {
			throw std::runtime_error("DuckDB only supports indices that fit on an uint32");
		}
		sel.set_index(row, indices[row]);
	}
}

template <class T>
static void SetMaskedSelectionVectorLoop(SelectionVector &sel, data_ptr_t indices_p, idx_t size, ValidityMask &mask,
                                         idx_t last_element_pos) {
	auto indices = (T *)indices_p;
	for (idx_t row = 0; row < size; row++) {
		if (mask.RowIsValid(row)) {
			sel.set_index(row, indices[row]);
		} else {
			//! Need to point out to last element
			sel.set_index(row, last_element_pos);
		}
	}
}

void SetSelectionVector(SelectionVector &sel, data_ptr_t indices_p, LogicalType &logical_type, idx_t size,
                        ValidityMask *mask = nullptr, idx_t last_element_pos = 0) {
	sel.Initialize(size);

	if (mask) {
		switch (logical_type.id()) {
		case LogicalTypeId::UTINYINT:
			SetMaskedSelectionVectorLoop<uint8_t>(sel, indices_p, size, *mask, last_element_pos);
			break;
		case LogicalTypeId::TINYINT:
			SetMaskedSelectionVectorLoop<int8_t>(sel, indices_p, size, *mask, last_element_pos);
			break;
		case LogicalTypeId::USMALLINT:
			SetMaskedSelectionVectorLoop<uint16_t>(sel, indices_p, size, *mask, last_element_pos);
			break;
		case LogicalTypeId::SMALLINT:
			SetMaskedSelectionVectorLoop<int16_t>(sel, indices_p, size, *mask, last_element_pos);
			break;
		case LogicalTypeId::UINTEGER:
			if (last_element_pos > NumericLimits<uint32_t>::Maximum()) {
				//! Its guaranteed that our indices will point to the last element, so just throw an error
				throw std::runtime_error("DuckDB only supports indices that fit on an uint32");
			}
			SetMaskedSelectionVectorLoop<uint32_t>(sel, indices_p, size, *mask, last_element_pos);
			break;
		case LogicalTypeId::INTEGER:
			SetMaskedSelectionVectorLoop<int32_t>(sel, indices_p, size, *mask, last_element_pos);
			break;
		case LogicalTypeId::UBIGINT:
			if (last_element_pos > NumericLimits<uint32_t>::Maximum()) {
				//! Its guaranteed that our indices will point to the last element, so just throw an error
				throw std::runtime_error("DuckDB only supports indices that fit on an uint32");
			}
			SetMaskedSelectionVectorLoop<uint64_t>(sel, indices_p, size, *mask, last_element_pos);
			break;
		case LogicalTypeId::BIGINT:
			if (last_element_pos > NumericLimits<uint32_t>::Maximum()) {
				//! Its guaranteed that our indices will point to the last element, so just throw an error
				throw std::runtime_error("DuckDB only supports indices that fit on an uint32");
			}
			SetMaskedSelectionVectorLoop<int64_t>(sel, indices_p, size, *mask, last_element_pos);
			break;

		default:
			throw std::runtime_error("(Arrow) Unsupported type for selection vectors " + logical_type.ToString());
		}

	} else {
		switch (logical_type.id()) {
		case LogicalTypeId::UTINYINT:
			SetSelectionVectorLoop<uint8_t>(sel, indices_p, size);
			break;
		case LogicalTypeId::TINYINT:
			SetSelectionVectorLoop<int8_t>(sel, indices_p, size);
			break;
		case LogicalTypeId::USMALLINT:
			SetSelectionVectorLoop<uint16_t>(sel, indices_p, size);
			break;
		case LogicalTypeId::SMALLINT:
			SetSelectionVectorLoop<int16_t>(sel, indices_p, size);
			break;
		case LogicalTypeId::UINTEGER:
			SetSelectionVectorLoop<uint32_t>(sel, indices_p, size);
			break;
		case LogicalTypeId::INTEGER:
			SetSelectionVectorLoop<int32_t>(sel, indices_p, size);
			break;
		case LogicalTypeId::UBIGINT:
			if (last_element_pos > NumericLimits<uint32_t>::Maximum()) {
				//! We need to check if our indexes fit in a uint32_t
				SetSelectionVectorLoopWithChecks<uint64_t>(sel, indices_p, size);
			} else {
				SetSelectionVectorLoop<uint64_t>(sel, indices_p, size);
			}
			break;
		case LogicalTypeId::BIGINT:
			if (last_element_pos > NumericLimits<uint32_t>::Maximum()) {
				//! We need to check if our indexes fit in a uint32_t
				SetSelectionVectorLoopWithChecks<int64_t>(sel, indices_p, size);
			} else {
				SetSelectionVectorLoop<int64_t>(sel, indices_p, size);
			}
			break;
		default:
			throw std::runtime_error("(Arrow) Unsupported type for selection vectors " + logical_type.ToString());
		}
	}
}

void ColumnArrowToDuckDBDictionary(Vector &vector, ArrowArray &array, ArrowScanState &scan_state, idx_t size,
                                   std::unordered_map<idx_t, unique_ptr<ArrowConvertData>> &arrow_convert_data,
                                   idx_t col_idx, idx_t list_col_idx = 0) {
	SelectionVector sel;
	auto &dict_vectors = scan_state.arrow_dictionary_vectors;
	if (dict_vectors.find(col_idx) == dict_vectors.end()) {
		//! We need to set the dictionary data for this column
		auto base_vector = make_unique<Vector>(vector.GetType());
		SetValidityMask(*base_vector, *array.dictionary, scan_state, array.dictionary->length, array.null_count > 0);
		ColumnArrowToDuckDB(*base_vector, *array.dictionary, scan_state, array.dictionary->length, arrow_convert_data,
		                    col_idx, list_col_idx);
		dict_vectors[col_idx] = move(base_vector);
	}
	auto dictionary_type = ((DictionaryArrowConvertData *)arrow_convert_data[col_idx].get())->dictionary_type;
	//! Get Pointer to Indices of Dictionary
	auto indices = (data_ptr_t)array.buffers[1] +
	               GetTypeIdSize(dictionary_type.InternalType()) * (scan_state.chunk_offset + array.offset);
	if (array.null_count > 0) {
		ValidityMask indices_validity;
		GetValidityMask(indices_validity, array, scan_state, size);
		SetSelectionVector(sel, indices, dictionary_type, size, &indices_validity, array.dictionary->length);
	} else {
		SetSelectionVector(sel, indices, dictionary_type, size);
	}
	vector.Slice(*dict_vectors[col_idx], sel, size);
}
void ArrowTableFunction::ArrowToDuckDB(ArrowScanState &scan_state,
                                       std::unordered_map<idx_t, unique_ptr<ArrowConvertData>> &arrow_convert_data,
                                       DataChunk &output) {
	for (idx_t col_idx = 0; col_idx < output.ColumnCount(); col_idx++) {
		idx_t list_col_idx = 0;
		auto &array = *scan_state.chunk->arrow_array.children[col_idx];
		if (!array.release) {
			throw InvalidInputException("arrow_scan: released array passed");
		}
		if (array.length != scan_state.chunk->arrow_array.length) {
			throw InvalidInputException("arrow_scan: array length mismatch");
		}
		if (array.dictionary) {
			ColumnArrowToDuckDBDictionary(output.data[col_idx], array, scan_state, output.size(), arrow_convert_data,
			                              col_idx);
		} else {
			SetValidityMask(output.data[col_idx], array, scan_state, output.size());
			ColumnArrowToDuckDB(output.data[col_idx], array, scan_state, output.size(), arrow_convert_data, col_idx,
			                    list_col_idx);
		}
	}
}

void ArrowTableFunction::ArrowScanFunction(ClientContext &context, const FunctionData *bind_data,
                                           FunctionOperatorData *operator_state, DataChunk *input, DataChunk &output) {
	auto &data = (ArrowScanFunctionData &)*bind_data;
	auto &state = (ArrowScanState &)*operator_state;

	//! have we run out of data on the current chunk? move to next one
	if (state.chunk_offset >= (idx_t)state.chunk->arrow_array.length) {
		state.chunk_offset = 0;
		state.arrow_dictionary_vectors.clear();
		state.chunk = data.stream->GetNextChunk();
	}

	//! have we run out of chunks? we are done
	if (!state.chunk->arrow_array.release) {
		return;
	}

	if ((idx_t)state.chunk->arrow_array.n_children != output.ColumnCount()) {
		throw InvalidInputException("arrow_scan: array column count mismatch");
	}
	int64_t output_size = MinValue<int64_t>(STANDARD_VECTOR_SIZE, state.chunk->arrow_array.length - state.chunk_offset);
	data.lines_read += output_size;
	output.SetCardinality(output_size);
	ArrowToDuckDB(state, data.arrow_convert_data, output);
	output.Verify();
	state.chunk_offset += output.size();
}

void ArrowTableFunction::ArrowScanFunctionParallel(ClientContext &context, const FunctionData *bind_data,
                                                   FunctionOperatorData *operator_state, DataChunk *input,
                                                   DataChunk &output, ParallelState *parallel_state_p) {
	auto &data = (ArrowScanFunctionData &)*bind_data;
	auto &state = (ArrowScanState &)*operator_state;
	//! Out of tuples in this chunk
	if (state.chunk_offset >= (idx_t)state.chunk->arrow_array.length) {
		return;
	}
	if ((idx_t)state.chunk->arrow_array.n_children != output.ColumnCount()) {
		throw InvalidInputException("arrow_scan: array column count mismatch");
	}
	int64_t output_size = MinValue<int64_t>(STANDARD_VECTOR_SIZE, state.chunk->arrow_array.length - state.chunk_offset);
	data.lines_read += output_size;
	output.SetCardinality(output_size);
	ArrowToDuckDB(state, data.arrow_convert_data, output);
	output.Verify();
	state.chunk_offset += output.size();
}

idx_t ArrowTableFunction::ArrowScanMaxThreads(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = (const ArrowScanFunctionData &)*bind_data_p;
	if (bind_data.stream->number_of_rows == -1) {
		return context.db->NumberOfThreads();
	}
	return (bind_data.stream->number_of_rows + bind_data.rows_per_thread - 1) / bind_data.rows_per_thread;
}

unique_ptr<ParallelState> ArrowTableFunction::ArrowScanInitParallelState(ClientContext &context,
                                                                         const FunctionData *bind_data_p) {
	return make_unique<ParallelArrowScanState>();
}

bool ArrowTableFunction::ArrowScanParallelStateNext(ClientContext &context, const FunctionData *bind_data_p,
                                                    FunctionOperatorData *operator_state,
                                                    ParallelState *parallel_state_p) {
	auto &bind_data = (const ArrowScanFunctionData &)*bind_data_p;
	auto &state = (ArrowScanState &)*operator_state;

	state.chunk_offset = 0;
	state.chunk = bind_data.stream->GetNextChunk();
	//! have we run out of chunks? we are done
	if (!state.chunk->arrow_array.release) {
		return false;
	}
	return true;
}

unique_ptr<FunctionOperatorData>
ArrowTableFunction::ArrowScanParallelInit(ClientContext &context, const FunctionData *bind_data_p, ParallelState *state,
                                          const vector<column_t> &column_ids, TableFilterCollection *filters) {
	auto current_chunk = make_unique<ArrowArrayWrapper>();
	auto result = make_unique<ArrowScanState>(move(current_chunk));
	result->column_ids = column_ids;
	if (!ArrowScanParallelStateNext(context, bind_data_p, result.get(), state)) {
		return nullptr;
	}
	return move(result);
}

unique_ptr<NodeStatistics> ArrowTableFunction::ArrowScanCardinality(ClientContext &context, const FunctionData *data) {
	auto &bind_data = (ArrowScanFunctionData &)*data;
	return make_unique<NodeStatistics>(bind_data.stream->number_of_rows, bind_data.stream->number_of_rows);
}

int ArrowTableFunction::ArrowProgress(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = (const ArrowScanFunctionData &)*bind_data_p;
	if (bind_data.stream->number_of_rows == 0) {
		return 100;
	}
	auto percentage = bind_data.lines_read * 100 / bind_data.stream->number_of_rows;
	return percentage;
}

void ArrowTableFunction::RegisterFunction(BuiltinFunctions &set) {
	TableFunctionSet arrow("arrow_scan");
	arrow.AddFunction(TableFunction({LogicalType::POINTER, LogicalType::POINTER, LogicalType::UBIGINT},
	                                ArrowScanFunction, ArrowScanBind, ArrowScanInit, nullptr, nullptr, nullptr,
	                                ArrowScanCardinality, nullptr, nullptr, ArrowScanMaxThreads,
	                                ArrowScanInitParallelState, ArrowScanFunctionParallel, ArrowScanParallelInit,
	                                ArrowScanParallelStateNext, false, false, ArrowProgress));
	set.AddFunction(arrow);
}

void BuiltinFunctions::RegisterArrowFunctions() {
	ArrowTableFunction::RegisterFunction(*this);
}
} // namespace duckdb
