#include <cstring> // strlen() on Solaris

#include "duckdb/common/types/vector.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/types/chunk_collection.hpp"

using namespace std;

namespace duckdb {

nullmask_t ZERO_MASK = nullmask_t(0);
const SelectionVector ConstantVector::ZeroSelectionVector = SelectionVector((sel_t*) ConstantVector::zero_vector);
const SelectionVector FlatVector::IncrementalSelectionVector = SelectionVector((sel_t*) FlatVector::incremental_vector);
constexpr sel_t ConstantVector::zero_vector[STANDARD_VECTOR_SIZE] = {0};
constexpr sel_t FlatVector::incremental_vector[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199, 200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219, 220, 221, 222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 236, 237, 238, 239, 240, 241, 242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255, 256, 257, 258, 259, 260, 261, 262, 263, 264, 265, 266, 267, 268, 269, 270, 271, 272, 273, 274, 275, 276, 277, 278, 279, 280, 281, 282, 283, 284, 285, 286, 287, 288, 289, 290, 291, 292, 293, 294, 295, 296, 297, 298, 299, 300, 301, 302, 303, 304, 305, 306, 307, 308, 309, 310, 311, 312, 313, 314, 315, 316, 317, 318, 319, 320, 321, 322, 323, 324, 325, 326, 327, 328, 329, 330, 331, 332, 333, 334, 335, 336, 337, 338, 339, 340, 341, 342, 343, 344, 345, 346, 347, 348, 349, 350, 351, 352, 353, 354, 355, 356, 357, 358, 359, 360, 361, 362, 363, 364, 365, 366, 367, 368, 369, 370, 371, 372, 373, 374, 375, 376, 377, 378, 379, 380, 381, 382, 383, 384, 385, 386, 387, 388, 389, 390, 391, 392, 393, 394, 395, 396, 397, 398, 399, 400, 401, 402, 403, 404, 405, 406, 407, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417, 418, 419, 420, 421, 422, 423, 424, 425, 426, 427, 428, 429, 430, 431, 432, 433, 434, 435, 436, 437, 438, 439, 440, 441, 442, 443, 444, 445, 446, 447, 448, 449, 450, 451, 452, 453, 454, 455, 456, 457, 458, 459, 460, 461, 462, 463, 464, 465, 466, 467, 468, 469, 470, 471, 472, 473, 474, 475, 476, 477, 478, 479, 480, 481, 482, 483, 484, 485, 486, 487, 488, 489, 490, 491, 492, 493, 494, 495, 496, 497, 498, 499, 500, 501, 502, 503, 504, 505, 506, 507, 508, 509, 510, 511, 512, 513, 514, 515, 516, 517, 518, 519, 520, 521, 522, 523, 524, 525, 526, 527, 528, 529, 530, 531, 532, 533, 534, 535, 536, 537, 538, 539, 540, 541, 542, 543, 544, 545, 546, 547, 548, 549, 550, 551, 552, 553, 554, 555, 556, 557, 558, 559, 560, 561, 562, 563, 564, 565, 566, 567, 568, 569, 570, 571, 572, 573, 574, 575, 576, 577, 578, 579, 580, 581, 582, 583, 584, 585, 586, 587, 588, 589, 590, 591, 592, 593, 594, 595, 596, 597, 598, 599, 600, 601, 602, 603, 604, 605, 606, 607, 608, 609, 610, 611, 612, 613, 614, 615, 616, 617, 618, 619, 620, 621, 622, 623, 624, 625, 626, 627, 628, 629, 630, 631, 632, 633, 634, 635, 636, 637, 638, 639, 640, 641, 642, 643, 644, 645, 646, 647, 648, 649, 650, 651, 652, 653, 654, 655, 656, 657, 658, 659, 660, 661, 662, 663, 664, 665, 666, 667, 668, 669, 670, 671, 672, 673, 674, 675, 676, 677, 678, 679, 680, 681, 682, 683, 684, 685, 686, 687, 688, 689, 690, 691, 692, 693, 694, 695, 696, 697, 698, 699, 700, 701, 702, 703, 704, 705, 706, 707, 708, 709, 710, 711, 712, 713, 714, 715, 716, 717, 718, 719, 720, 721, 722, 723, 724, 725, 726, 727, 728, 729, 730, 731, 732, 733, 734, 735, 736, 737, 738, 739, 740, 741, 742, 743, 744, 745, 746, 747, 748, 749, 750, 751, 752, 753, 754, 755, 756, 757, 758, 759, 760, 761, 762, 763, 764, 765, 766, 767, 768, 769, 770, 771, 772, 773, 774, 775, 776, 777, 778, 779, 780, 781, 782, 783, 784, 785, 786, 787, 788, 789, 790, 791, 792, 793, 794, 795, 796, 797, 798, 799, 800, 801, 802, 803, 804, 805, 806, 807, 808, 809, 810, 811, 812, 813, 814, 815, 816, 817, 818, 819, 820, 821, 822, 823, 824, 825, 826, 827, 828, 829, 830, 831, 832, 833, 834, 835, 836, 837, 838, 839, 840, 841, 842, 843, 844, 845, 846, 847, 848, 849, 850, 851, 852, 853, 854, 855, 856, 857, 858, 859, 860, 861, 862, 863, 864, 865, 866, 867, 868, 869, 870, 871, 872, 873, 874, 875, 876, 877, 878, 879, 880, 881, 882, 883, 884, 885, 886, 887, 888, 889, 890, 891, 892, 893, 894, 895, 896, 897, 898, 899, 900, 901, 902, 903, 904, 905, 906, 907, 908, 909, 910, 911, 912, 913, 914, 915, 916, 917, 918, 919, 920, 921, 922, 923, 924, 925, 926, 927, 928, 929, 930, 931, 932, 933, 934, 935, 936, 937, 938, 939, 940, 941, 942, 943, 944, 945, 946, 947, 948, 949, 950, 951, 952, 953, 954, 955, 956, 957, 958, 959, 960, 961, 962, 963, 964, 965, 966, 967, 968, 969, 970, 971, 972, 973, 974, 975, 976, 977, 978, 979, 980, 981, 982, 983, 984, 985, 986, 987, 988, 989, 990, 991, 992, 993, 994, 995, 996, 997, 998, 999, 1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010, 1011, 1012, 1013, 1014, 1015, 1016, 1017, 1018, 1019, 1020, 1021, 1022, 1023};

Vector::Vector(const VectorCardinality &vcardinality, TypeId type, bool create_data, bool zero_data)
    : vector_type(VectorType::FLAT_VECTOR), type(type), vcardinality(vcardinality), data(nullptr) {
	if (create_data) {
		Initialize(type, zero_data);
	}
}

Vector::Vector(const VectorCardinality &vcardinality, TypeId type) : Vector(vcardinality, type, true, false) {
}

Vector::Vector(const VectorCardinality &vcardinality, TypeId type, data_ptr_t dataptr)
    : vector_type(VectorType::FLAT_VECTOR), type(type), vcardinality(vcardinality), data(dataptr) {
	if (dataptr && type == TypeId::INVALID) {
		throw InvalidTypeException(type, "Cannot create a vector of type INVALID!");
	}
}

Vector::Vector(const VectorCardinality &vcardinality, Value value)
    : vector_type(VectorType::CONSTANT_VECTOR), vcardinality(vcardinality) {
	Reference(value);
}

Vector::Vector(const VectorCardinality &vcardinality)
    : vector_type(VectorType::FLAT_VECTOR), type(TypeId::INVALID), vcardinality(vcardinality), data(nullptr) {
}

Vector::Vector(Vector &&other) noexcept
    : vector_type(other.vector_type), type(other.type), vcardinality(other.vcardinality),
      data(other.data), nullmask(other.nullmask), buffer(move(other.buffer)), auxiliary(move(other.auxiliary)) {
}

void Vector::Reference(Value &value) {
	vector_type = VectorType::CONSTANT_VECTOR;
	type = value.type;
	buffer = VectorBuffer::CreateConstantVector(type);
	data = buffer->GetData();
	SetValue(0, value);
}

void Vector::Reference(Vector &other) {
	vector_type = other.vector_type;
	buffer = other.buffer;
	data = other.data;
	type = other.type;
	nullmask = other.nullmask;
	auxiliary = nullptr;

	switch (type) {
	case TypeId::STRUCT:
		for (auto &child : StructVector::GetEntries(other)) {
			auto child_copy = make_unique<Vector>(cardinality(), child.second->type);
			child_copy->Reference(*child.second);
			StructVector::AddEntry(*this, child.first, move(child_copy));
		}
		break;

	default:
		auxiliary = other.auxiliary;
		break;
	}
}

void Vector::Slice(Vector &other, idx_t offset) {
	if (other.vector_type == VectorType::CONSTANT_VECTOR) {
		Reference(other);
		return;
	}
	assert(other.vector_type == VectorType::FLAT_VECTOR);

	// create a reference to the other vector
	Reference(other);
	if (offset > 0) {
		data = data + GetTypeIdSize(type) * offset;
		nullmask <<= offset;
	}
}

void Vector::Slice(Vector &other, SelectionVector &sel) {
	Reference(other);
	Slice(sel);
}

void Vector::Slice(SelectionVector &sel) {
	assert(vector_type != VectorType::DICTIONARY_VECTOR);
	if (vector_type == VectorType::CONSTANT_VECTOR) {
		// dictionary on a constant is just a constant
		return;
	}
	auto child_ref = make_buffer<VectorChildBuffer>(vcardinality);
	child_ref->data.Reference(*this);

	auto dict_buffer = make_unique<DictionaryBuffer>(sel);
	buffer = move(dict_buffer);
	auxiliary = move(child_ref);
	vector_type = VectorType::DICTIONARY_VECTOR;
}

void Vector::Initialize(TypeId new_type, bool zero_data) {
	if (new_type != TypeId::INVALID) {
		type = new_type;
	}
	buffer.reset();
	auxiliary.reset();
	nullmask.reset();
	if (GetTypeIdSize(type) > 0) {
		buffer = VectorBuffer::CreateStandardVector(type);
		data = buffer->GetData();
		if (zero_data) {
			memset(data, 0, STANDARD_VECTOR_SIZE * GetTypeIdSize(type));
		}
	}
}

void Vector::SetValue(idx_t index, Value val) {
	if (vector_type == VectorType::DICTIONARY_VECTOR) {
		// dictionary: apply dictionary and forward to child
		auto &sel_vector = DictionaryVector::SelectionVector(*this);
		auto &child = DictionaryVector::Child(*this);
		return child.SetValue(sel_vector.get_index(index), move(val));
	}
	Value newVal = val.CastAs(type);

	nullmask[index] = newVal.is_null;
	if (newVal.is_null) {
		return;
	}
	switch (type) {
	case TypeId::BOOL:
		((int8_t *)data)[index] = newVal.value_.boolean;
		break;
	case TypeId::INT8:
		((int8_t *)data)[index] = newVal.value_.tinyint;
		break;
	case TypeId::INT16:
		((int16_t *)data)[index] = newVal.value_.smallint;
		break;
	case TypeId::INT32:
		((int32_t *)data)[index] = newVal.value_.integer;
		break;
	case TypeId::INT64:
		((int64_t *)data)[index] = newVal.value_.bigint;
		break;
	case TypeId::FLOAT:
		((float *)data)[index] = newVal.value_.float_;
		break;
	case TypeId::DOUBLE:
		((double *)data)[index] = newVal.value_.double_;
		break;
	case TypeId::POINTER:
		((uintptr_t *)data)[index] = newVal.value_.pointer;
		break;
	case TypeId::VARCHAR: {
		((string_t *)data)[index] = StringVector::AddString(*this, newVal.str_value);
		break;
	}
	case TypeId::STRUCT: {
		if (!auxiliary || StructVector::GetEntries(*this).size() == 0) {
			for (size_t i = 0; i < val.struct_value.size(); i++) {
				auto &struct_child = val.struct_value[i];
				auto cv = make_unique<Vector>(vcardinality, struct_child.second.type);
				cv->vector_type = vector_type;
				StructVector::AddEntry(*this, struct_child.first, move(cv));
			}
		}

		auto &children = StructVector::GetEntries(*this);
		assert(children.size() == val.struct_value.size());

		for (size_t i = 0; i < val.struct_value.size(); i++) {
			auto &struct_child = val.struct_value[i];
			assert(vector_type == VectorType::CONSTANT_VECTOR || vector_type == VectorType::FLAT_VECTOR);
			auto &vec_child = children[i];
			assert(vec_child.first == struct_child.first);
			vec_child.second->SetValue(index, struct_child.second);
		}
	} break;

	case TypeId::LIST: {
		if (!auxiliary) {
			auto cc = make_unique<ChunkCollection>();
			ListVector::SetEntry(*this, move(cc));
		}
		auto &child_cc = ListVector::GetEntry(*this);
		// TODO optimization: in-place update if fits
		auto offset = child_cc.count;
		if (val.list_value.size() > 0) {
			idx_t append_idx = 0;
			while (append_idx < val.list_value.size()) {
				idx_t this_append_len = min((idx_t)STANDARD_VECTOR_SIZE, val.list_value.size() - append_idx);

				DataChunk child_append_chunk;
				child_append_chunk.count = this_append_len;
				vector<TypeId> types;
				types.push_back(val.list_value[0].type);
				child_append_chunk.Initialize(types);
				for (idx_t i = 0; i < this_append_len; i++) {
					child_append_chunk.data[0].SetValue(i, val.list_value[i + append_idx]);
				}
				child_cc.Append(child_append_chunk);
				append_idx += this_append_len;
			}
		}
		// now set the pointer
		auto &entry = ((list_entry_t *)data)[index];
		entry.length = val.list_value.size();
		entry.offset = offset;
	} break;
	default:
		throw NotImplementedException("Unimplemented type for Vector::SetValue");
	}
}

Value Vector::GetValue(idx_t index) const {
	if (vector_type == VectorType::CONSTANT_VECTOR) {
		index = 0;
	} else if (vector_type == VectorType::DICTIONARY_VECTOR) {
		// dictionary: apply dictionary and forward to child
		auto &sel_vector = DictionaryVector::SelectionVector(*this);
		auto &child = DictionaryVector::Child(*this);
		return child.GetValue(sel_vector.get_index(index));
	} else {
		assert(vector_type == VectorType::FLAT_VECTOR);
	}

	if (nullmask[index]) {
		return Value(type);
	}
	switch (type) {
	case TypeId::BOOL:
		return Value::BOOLEAN(((int8_t *)data)[index]);
	case TypeId::INT8:
		return Value::TINYINT(((int8_t *)data)[index]);
	case TypeId::INT16:
		return Value::SMALLINT(((int16_t *)data)[index]);
	case TypeId::INT32:
		return Value::INTEGER(((int32_t *)data)[index]);
	case TypeId::INT64:
		return Value::BIGINT(((int64_t *)data)[index]);
	case TypeId::HASH:
		return Value::HASH(((uint64_t *)data)[index]);
	case TypeId::POINTER:
		return Value::POINTER(((uintptr_t *)data)[index]);
	case TypeId::FLOAT:
		return Value::FLOAT(((float *)data)[index]);
	case TypeId::DOUBLE:
		return Value::DOUBLE(((double *)data)[index]);
	case TypeId::VARCHAR: {
		auto str = ((string_t *)data)[index];
		return Value(str.GetString());
	}
	case TypeId::STRUCT: {
		Value ret(TypeId::STRUCT);
		ret.is_null = false;
		// we can derive the value schema from the vector schema
		for (auto &struct_child : StructVector::GetEntries(*this)) {
			ret.struct_value.push_back(pair<string, Value>(struct_child.first, struct_child.second->GetValue(index)));
		}
		return ret;
	}
	case TypeId::LIST: {
		Value ret(TypeId::LIST);
		ret.is_null = false;
		auto offlen = ((list_entry_t *)data)[index];
		auto &child_cc = ListVector::GetEntry(*this);
		for (idx_t i = offlen.offset; i < offlen.offset + offlen.length; i++) {
			ret.list_value.push_back(child_cc.GetValue(0, i));
		}
		return ret;
	}
	default:
		throw NotImplementedException("Unimplemented type for value access");
	}
}

string VectorTypeToString(VectorType type) {
	switch (type) {
	case VectorType::FLAT_VECTOR:
		return "FLAT";
	case VectorType::SEQUENCE_VECTOR:
		return "SEQUENCE";
	case VectorType::DICTIONARY_VECTOR:
		return "DICTIONARY";
	case VectorType::CONSTANT_VECTOR:
		return "CONSTANT";
	default:
		return "UNKNOWN";
	}
}

string Vector::ToString() const {
	auto count = size();

	string retval = VectorTypeToString(vector_type) + " " + TypeIdToString(type) + ": " + to_string(count) + " = [ ";
	switch (vector_type) {
	case VectorType::FLAT_VECTOR:
	case VectorType::DICTIONARY_VECTOR:
		for (idx_t i = 0; i < count; i++) {
			retval += GetValue(i).ToString() + (i == count - 1 ? "" : ", ");
		}
		break;
	case VectorType::CONSTANT_VECTOR:
		retval += GetValue(0).ToString();
		break;
	case VectorType::SEQUENCE_VECTOR: {
		int64_t start, increment;
		SequenceVector::GetSequence(*this, start, increment);
		for (idx_t i = 0; i < count; i++) {
			retval += to_string(start + increment * i) + (i == count - 1 ? "" : ", ");
		}
		break;
	}
	default:
		retval += "UNKNOWN VECTOR TYPE";
		break;
	}
	retval += "]";
	return retval;
}

void Vector::Print() {
	Printer::Print(ToString());
}

template <class T>
static void flatten_constant_vector_loop(data_ptr_t data, data_ptr_t old_data, idx_t count) {
	auto constant = *((T *)old_data);
	auto output = (T *)data;
	for(idx_t i = 0; i < count; i++) {
		output[i] = constant;
	}
}

// void Vector::Normalify(SelectionVector &sel) {
// }

void Vector::Normalify() {
	auto count = size();

	switch (vector_type) {
	case VectorType::FLAT_VECTOR:
		// already a flat vector
		break;
	case VectorType::DICTIONARY_VECTOR: {
		throw NotImplementedException("FIXME: flatten dicitonary vector");

		// // create a new vector with the same size, but without a selection vector
		// VectorCardinality other_cardinality(size());
		// Vector other(other_cardinality, type);
		// // now copy the data of this vector to the other vector, removing the selection vector in the process
		// VectorOperations::Copy(*this, other);
		// // create a reference to the data in the other vector
		// this->Reference(other);
		break;
	}
	case VectorType::CONSTANT_VECTOR: {
		vector_type = VectorType::FLAT_VECTOR;
		// allocate a new buffer for the vector
		auto old_buffer = move(buffer);
		auto old_data = data;
		buffer = VectorBuffer::CreateStandardVector(type);
		data = buffer->GetData();
		if (nullmask[0]) {
			// constant NULL, set nullmask
			nullmask.set();
			return;
		}
		// non-null constant: have to repeat the constant
		switch (type) {
		case TypeId::BOOL:
		case TypeId::INT8:
			flatten_constant_vector_loop<int8_t>(data, old_data, count);
			break;
		case TypeId::INT16:
			flatten_constant_vector_loop<int16_t>(data, old_data, count);
			break;
		case TypeId::INT32:
			flatten_constant_vector_loop<int32_t>(data, old_data, count);
			break;
		case TypeId::INT64:
			flatten_constant_vector_loop<int64_t>(data, old_data, count);
			break;
		case TypeId::FLOAT:
			flatten_constant_vector_loop<float>(data, old_data, count);
			break;
		case TypeId::DOUBLE:
			flatten_constant_vector_loop<double>(data, old_data, count);
			break;
		case TypeId::HASH:
			flatten_constant_vector_loop<uint64_t>(data, old_data, count);
			break;
		case TypeId::POINTER:
			flatten_constant_vector_loop<uintptr_t>(data, old_data, count);
			break;
		case TypeId::VARCHAR:
			flatten_constant_vector_loop<string_t>(data, old_data, count);
			break;
		case TypeId::LIST: {
			flatten_constant_vector_loop<list_entry_t>(data, old_data, count);
			break;
		}
		case TypeId::STRUCT: {
			for (auto &child : StructVector::GetEntries(*this)) {
				assert(child.second->vector_type == VectorType::CONSTANT_VECTOR);
				child.second->Normalify();
			}
		} break;
		default:
			throw NotImplementedException("Unimplemented type for VectorOperations::Normalify");
		}
		break;
	}
	case VectorType::SEQUENCE_VECTOR: {
		int64_t start, increment;
		SequenceVector::GetSequence(*this, start, increment);

		vector_type = VectorType::FLAT_VECTOR;
		buffer = VectorBuffer::CreateStandardVector(type);
		data = buffer->GetData();
		VectorOperations::GenerateSequence(*this, start, increment);
		break;
	}
	default:
		throw NotImplementedException("FIXME: unimplemented type for normalify");
	}
}

void Vector::Orrify(VectorData &data) {
	switch (vector_type) {
	case VectorType::DICTIONARY_VECTOR: {
		auto &sel = DictionaryVector::SelectionVector(*this);
		auto &child = DictionaryVector::Child(*this);
		child.Normalify();

		data.sel = &sel;
		data.data = FlatVector::GetData(child);
		data.nullmask = &child.nullmask;
		break;
	}
	case VectorType::CONSTANT_VECTOR:
		data.sel = &ConstantVector::ZeroSelectionVector;
		data.data = ConstantVector::GetData(*this);
		data.nullmask = &nullmask;
		break;
	default:
		Normalify();
		data.sel = &FlatVector::IncrementalSelectionVector;
		data.data = FlatVector::GetData(*this);
		data.nullmask = &nullmask;
		break;
	}
}

void Vector::Sequence(int64_t start, int64_t increment) {
	vector_type = VectorType::SEQUENCE_VECTOR;
	this->buffer = make_buffer<VectorBuffer>(sizeof(int64_t) * 2);
	auto data = (int64_t *)buffer->GetData();
	data[0] = start;
	data[1] = increment;
	nullmask.reset();
	auxiliary.reset();
}

void Vector::Verify() {
	if (size() == 0) {
		return;
	}
#ifdef DEBUG
	if (type == TypeId::VARCHAR) {
		// we just touch all the strings and let the sanitizer figure out if any
		// of them are deallocated/corrupt
		switch(vector_type) {
		case VectorType::CONSTANT_VECTOR: {
			auto string = ConstantVector::GetData<string_t>(*this);
			if (!ConstantVector::IsNull(*this)) {
				string->Verify();
			}
			break;
		}
		case VectorType::FLAT_VECTOR: {
			auto strings = FlatVector::GetData<string_t>(*this);
			for(idx_t i = 0; i < size(); i++) {
				if (!nullmask[i]) {
					strings[i].Verify();
				}
			}
			break;
		}
		case VectorType::DICTIONARY_VECTOR: {
			auto &child = DictionaryVector::Child(*this);
			child.Verify();
			break;
		}
		default:
			break;
		}
	}

	if (type == TypeId::STRUCT) {
		auto &children = StructVector::GetEntries(*this);
		assert(children.size() > 0);
		for (auto &child : children) {
			assert(child.second->SameCardinality(*this));
			child.second->Verify();
		}
	}

	if (type == TypeId::LIST) {
		if (vector_type == VectorType::CONSTANT_VECTOR) {
			if (!ConstantVector::IsNull(*this)) {
				ListVector::GetEntry(*this).Verify();
				auto le = ConstantVector::GetData<list_entry_t>(*this);
				assert(le->offset + le->length <= ListVector::GetEntry(*this).count);
			}
		} else if (vector_type == VectorType::FLAT_VECTOR) {
			if (VectorOperations::HasNotNull(*this)) {
				ListVector::GetEntry(*this).Verify();
			}
			auto list_data = FlatVector::GetData<list_entry_t>(*this);
			for(idx_t i = 0; i < size(); i++) {
				auto &le = list_data[i];
				if (!nullmask[i]) {
					assert(le.offset + le.length <= ListVector::GetEntry(*this).count);
				}
			}
		}
	}
// TODO verify list and struct
#endif
}

void StringVector::MoveStringsToHeap(Vector &vector, StringHeap &heap, SelectionVector &sel) {
	assert(vector.type == TypeId::VARCHAR);

	vector.Normalify();

	assert(vector.vector_type == VectorType::FLAT_VECTOR);
	auto source_strings = FlatVector::GetData<string_t>(vector);
	auto old_buffer = move(vector.buffer);

	vector.buffer = VectorBuffer::CreateStandardVector(TypeId::VARCHAR);
	vector.data = vector.buffer->GetData();
	auto target_strings = FlatVector::GetData<string_t>(vector);
	auto &nullmask = FlatVector::Nullmask(vector);
	for(idx_t i = 0; i < vector.size(); i++) {
		auto idx = sel.get_index(i);
		if (!nullmask[idx] && !source_strings[idx].IsInlined()) {
			target_strings[idx] = heap.AddString(source_strings[idx]);
		} else {
			target_strings[idx] = source_strings[idx];
		}
	}
}

void StringVector::MoveStringsToHeap(Vector &vector, StringHeap &heap) {
	assert(vector.type == TypeId::VARCHAR);

	switch(vector.vector_type) {
	case VectorType::CONSTANT_VECTOR: {
		auto source_strings = ConstantVector::GetData<string_t>(vector);
		if (ConstantVector::IsNull(vector) || source_strings[0].IsInlined()) {
			return;
		}
		auto old_buffer = move(vector.buffer);

		vector.buffer = VectorBuffer::CreateConstantVector(TypeId::VARCHAR);
		vector.data = vector.buffer->GetData();
		auto target_strings = ConstantVector::GetData<string_t>(vector);
		target_strings[0] = heap.AddString(source_strings[0]);
		break;
	}
	case VectorType::FLAT_VECTOR: {
		auto source_strings = FlatVector::GetData<string_t>(vector);
		auto old_buffer = move(vector.buffer);

		vector.buffer = VectorBuffer::CreateStandardVector(TypeId::VARCHAR);
		vector.data = vector.buffer->GetData();
		auto target_strings = FlatVector::GetData<string_t>(vector);
		for(idx_t i = 0; i < vector.size(); i++) {
			if (!vector.nullmask[i] && !source_strings[i].IsInlined()) {
				target_strings[i] = heap.AddString(source_strings[i]);
			} else {
				target_strings[i] = source_strings[i];
			}
		}
		break;
	}
	case VectorType::DICTIONARY_VECTOR: {
		auto &sel = DictionaryVector::SelectionVector(vector);
		auto &child = DictionaryVector::Child(vector);;
		StringVector::MoveStringsToHeap(child, heap, sel);
		break;
	}
	default:
		throw NotImplementedException("");
	}
}

string_t StringVector::AddString(Vector &vector, const char *data, idx_t len) {
	return StringVector::AddString(vector, string_t(data, len));
}

string_t StringVector::AddString(Vector &vector, const char *data) {
	return StringVector::AddString(vector, string_t(data, strlen(data)));
}

string_t StringVector::AddString(Vector &vector, const string &data) {
	return StringVector::AddString(vector, string_t(data.c_str(), data.size()));
}

string_t StringVector::AddString(Vector &vector, string_t data) {
	assert(vector.type == TypeId::VARCHAR);
	if (data.IsInlined()) {
		// string will be inlined: no need to store in string heap
		return data;
	}
	if (!vector.auxiliary) {
		vector.auxiliary = make_buffer<VectorStringBuffer>();
	}
	assert(vector.auxiliary->type == VectorBufferType::STRING_BUFFER);
	auto &string_buffer = (VectorStringBuffer &)*vector.auxiliary;
	return string_buffer.AddString(data);
}

string_t StringVector::EmptyString(Vector &vector, idx_t len) {
	assert(vector.type == TypeId::VARCHAR);
	if (len < string_t::INLINE_LENGTH) {
		return string_t(len);
	}
	if (!vector.auxiliary) {
		vector.auxiliary = make_buffer<VectorStringBuffer>();
	}
	assert(vector.auxiliary->type == VectorBufferType::STRING_BUFFER);
	auto &string_buffer = (VectorStringBuffer &)*vector.auxiliary;
	return string_buffer.EmptyString(len);
}

void StringVector::AddHeapReference(Vector &vector, Vector &other) {
	assert(vector.type == TypeId::VARCHAR);
	assert(other.type == TypeId::VARCHAR);

	if (!other.auxiliary) {
		return;
	}
	if (!vector.auxiliary) {
		vector.auxiliary = make_buffer<VectorStringBuffer>();
	}
	assert(vector.auxiliary->type == VectorBufferType::STRING_BUFFER);
	assert(other.auxiliary->type == VectorBufferType::STRING_BUFFER);
	auto &string_buffer = (VectorStringBuffer &)*vector.auxiliary;
	string_buffer.AddHeapReference(other.auxiliary);
}

child_list_t<unique_ptr<Vector>> &StructVector::GetEntries(const Vector &vector) {
	assert(vector.type == TypeId::STRUCT);
	assert(vector.auxiliary);
	assert(vector.auxiliary->type == VectorBufferType::STRUCT_BUFFER);
	return ((VectorStructBuffer *)vector.auxiliary.get())->GetChildren();
}

void StructVector::AddEntry(Vector &vector, string name, unique_ptr<Vector> entry) {
	// TODO asser that an entry with this name does not already exist
	assert(vector.type == TypeId::STRUCT);
	if (!vector.auxiliary) {
		vector.auxiliary = make_buffer<VectorStructBuffer>();
	}
	assert(vector.auxiliary);
	assert(vector.auxiliary->type == VectorBufferType::STRUCT_BUFFER);
	((VectorStructBuffer *)vector.auxiliary.get())->AddChild(name, move(entry));
}

bool ListVector::HasEntry(const Vector &vector) {
	assert(vector.type == TypeId::LIST);
	return vector.auxiliary != nullptr;
}

ChunkCollection &ListVector::GetEntry(const Vector &vector) {
	assert(vector.type == TypeId::LIST);
	assert(vector.auxiliary);
	assert(vector.auxiliary->type == VectorBufferType::LIST_BUFFER);
	return ((VectorListBuffer *)vector.auxiliary.get())->GetChild();
}

void ListVector::SetEntry(Vector &vector, unique_ptr<ChunkCollection> cc) {
	assert(vector.type == TypeId::LIST);
	if (!vector.auxiliary) {
		vector.auxiliary = make_buffer<VectorListBuffer>();
	}
	assert(vector.auxiliary);
	assert(vector.auxiliary->type == VectorBufferType::LIST_BUFFER);
	((VectorListBuffer *)vector.auxiliary.get())->SetChild(move(cc));
}

} // namespace duckdb
