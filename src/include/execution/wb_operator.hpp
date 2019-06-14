#pragma once

//! A chunk of the table
class DataChunk {
public:
	//! Creates an empty DataChunk
	DataChunk();

	//! The amount of vectors that are part of this DataChunk.
	count_t column_count;
	//! The vectors owned by the DataChunk.
	unique_ptr<Vector[]> data;

private:
	//! The data owned by this DataChunk. This data is referenced by
	//! the member vectors.
	unique_ptr<data_t[]> owned_data;
};

class Vector {
	friend class DataChunk;

public:
	//! The type of the elements stored in the vector.
	TypeId type;
	//! The amount of elements in the vector.
	count_t count;
	//! A pointer to the data.
	data_ptr_t data;

	Vector();
	//! Create a vector of size one holding the passed on value
	Vector(Value value);

protected:
	//! If the vector owns data, this is the unique_ptr holds the actual data.
	unique_ptr<data_t[]> owned_data;
};

class CompressionOperator {

	void Transform(DataChunk chunk) {
	}
};

class Sampler {
	Sampler(Column *column, size_t column_size);
	Vector GetSample();
	size_t rows;
};

class PatternDetector {

	virtual double
	Evaluate(); // returns a coverage between 0 and 1 regarding the amount of tuples that apply the pattern
};

class NullPatternDetector : PatternDetector {
	double Evaluate();
};
class NumberAsStringDetector : PatternDetector {
	double Evaluate();
};
class CharSetSplitDetector : PatternDetector {
	double Evaluate();
};

class PatternDetectorEngine {
	PatternDetectorEngine(vector<Column> column, vector<PatternDetector> pattern_detectors);

	void ExecuteDetection();
};

class ExpressionTree {};