#pragma once
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/parser/parsed_data/create_sequence_info.hpp"

namespace duckdb {

class SequenceOption {
public:
	SequenceOption(SequenceInfo type_p) : type(type_p) {
	}

public:
	SequenceInfo type;
};

class ValueSequenceOption : public SequenceOption {
public:
	ValueSequenceOption(SequenceInfo type, Value value_p) : SequenceOption(type), value(value_p) {
	}

public:
	Value value;
};

class QualifiedSequenceOption : public SequenceOption {
public:
	QualifiedSequenceOption(SequenceInfo type, QualifiedName qualified_name_p)
	    : SequenceOption(type), qualified_name(qualified_name_p) {
	}

public:
	QualifiedName qualified_name;
};

} // namespace duckdb
