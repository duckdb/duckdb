#pragma once

namespace duckdb {

class StripeStreams {
public:
	virtual ~StripeStreams() = default;

	/**
	 * get column selector for current stripe reading session
	 * @return column selector will hold column projection info
	 */
	virtual const dwio::common::ColumnSelector &getColumnSelector() const = 0;

	// Get row reader options
	virtual const dwio::common::RowReaderOptclass StripeStreams {
	public:
		virtual ~StripeStreams() = default;

		/**
		 * get column selector for current stripe reading session
		 * @return column selector will hold column projection info
		 */
		virtual const dwio::common::ColumnSelector &getColumnSelector() const = 0;

		// Get row reader options
		virtual const dwio::common::RowReaderOptions &getRowReaderOptions() const = 0;

		/**
		 * Get the encoding for the given column for this stripe.
		 */
		virtual const proto::ColumnEncoding &getEncoding(const EncodingKey &) const = 0;

		/**
		 * Get the stream for the given column/kind in this stripe.
		 * @param streamId stream identifier object
		 * @param throwIfNotFound fail if a stream is required and not found
		 * @return the new stream
		 */
		virtual unique_ptr<SeekableInputStream> getStream(const StreamIdentifier &si, bool throwIfNotFound) const = 0;

		/**
		 * visit all streams of given node and execute visitor logic
		 * return number of streams visited
		 */
		virtual uint32_t visitStreamsOfNode(uint32_t node, std::function<void(const StreamInformation &)> visitor)
		    const = 0;

		/**
		 * Get the value of useVInts for the given column in this stripe.
		 * Defaults to true.
		 * @param streamId stream identifier
		 */
		virtual bool getUseVInts(const StreamIdentifier &streamId) const = 0;

		/**
		 * Get the memory pool for this reader.
		 */
		virtual memory::MemoryPool &getMemoryPool() const = 0;

		/**
		 * Get the RowGroupIndex.
		 * @return a vector of RowIndex belonging to the stripe
		 */
		virtual unique_ptr<proto::RowIndex> getRowGroupIndex(const StreamIdentifier &si) const = 0;

		/**
		 * Get stride index provider which is used by string dictionary reader to
		 * get the row index stride index where next() happens
		 */
		virtual const StrideIndexProvider &getStrideIndexProvider() const = 0;
	}
	ions &getRowReaderOptions() const = 0;

	/**
	 * Get the encoding for the given column for this stripe.
	 */
	virtual const proto::ColumnEncoding &getEncoding(const EncodingKey &) const = 0;

	/**
	 * Get the stream for the given column/kind in this stripe.
	 * @param streamId stream identifier object
	 * @param throwIfNotFound fail if a stream is required and not found
	 * @return the new stream
	 */
	virtual unique_ptr<SeekableInputStream> getStream(const StreamIdentifier &si, bool throwIfNotFound) const = 0;

	/**
	 * visit all streams of given node and execute visitor logic
	 * return number of streams visited
	 */
	virtual uint32_t visitStreamsOfNode(uint32_t node,
	                                    std::function<void(const StreamInformation &)> visitor) const = 0;

	/**
	 * Get the value of useVInts for the given column in this stripe.
	 * Defaults to true.
	 * @param streamId stream identifier
	 */
	virtual bool getUseVInts(const StreamIdentifier &streamId) const = 0;

	/**
	 * Get the memory pool for this reader.
	 */
	virtual memory::MemoryPool &getMemoryPool() const = 0;

	/**
	 * Get the RowGroupIndex.
	 * @return a vector of RowIndex belonging to the stripe
	 */
	virtual unique_ptr<proto::RowIndex> getRowGroupIndex(const StreamIdentifier &si) const = 0;

	/**
	 * Get stride index provider which is used by string dictionary reader to
	 * get the row index stride index where next() happens
	 */
	virtual const StrideIndexProvider &getStrideIndexProvider() const = 0;
};

class ColumnReader {

public:
	ColumnReader(const EncodingKey &ek, StripeStreams &stripe);

	virtual ~ColumnReader() = default;

	/**
	 * Skip number of specified rows.
	 * @param numValues the number of values to skip
	 * @return the number of non-null values skipped
	 */
	virtual uint64_t skip(uint64_t numValues);

	/**
	 * Read the next group of values into a RowVector.
	 * @param numValues the number of values to read
	 * @param vector to read into
	 */
	virtual void next(uint64_t numValues, VectorPtr &result, const uint64_t *nulls = nullptr) = 0;
};

class SelectiveColumnReader : public ColumnReader {
public:
	static constexpr uint64_t kStringBufferSize = 16 * 1024;

	SelectiveColumnReader(const EncodingKey &ek, StripeStreams &stripe, common::ScanSpec *scanSpec);

	/**
	 * Read the next group of values into a RowVector.
	 * @param numValues the number of values to read
	 * @param vector to read into
	 */
	void next(uint64_t /*numValues*/, VectorPtr & /*result*/, const uint64_t * /*incomingNulls*/) override {
		DATALIB_CHECK(false) << "next() is only defined in SelectiveStructColumnReader";
	}

	// Creates a reader for the given stripe.
	static unique_ptr<SelectiveColumnReader> build(const std::shared_ptr<const dwio::common::TypeWithId> &requestedType,
	                                               const std::shared_ptr<const dwio::common::TypeWithId> &dataType,
	                                               StripeStreams &stripe, common::ScanSpec *scanSpec,
	                                               uint32_t sequence = 0);

	// Seeks to offset and reads the rows in 'rows' and applies
	// filters and value processing as given by 'scanSpec supplied at
	// construction. 'offset' is relative to start of stripe. 'rows' are
	// relative to 'offset', so that row 0 is the 'offset'th row from
	// start of stripe. 'rows' is expected to stay constant
	// between this and the next call to read.
	virtual void read(vector_size_t offset, RowSet rows, const uint64_t *incomingNulls) = 0;

	// Extracts the values at 'rows' into '*result'. May rewrite or
	// reallocate '*result'. 'rows' must be the same set or a subset of
	// 'rows' passed to the last 'read().
	virtual void getValues(RowSet rows, VectorPtr *result) = 0;

	// Returns the rows that were selected/visited by the last
	// read(). If 'this' has no filter, returns 'rows' passed to last
	// read().
	const RowSet outputRows() const {
		if (scanSpec_->hasFilter()) {
			return outputRows_;
		}
		return inputRows_;
	}

	// Advances to 'offset', so that the next item to be read is the
	// offset-th from the start of stripe.
	void seekTo(vector_size_t offset, bool readsNullsOnly);

	// The below functions are called from ColumnVisitor to fill the result set.
	inline void addOutputRow(vector_size_t row) {
		outputRows_.push_back(row);
	}

	template <typename T>
	inline void addNull() {
		DATALIB_DCHECK(rawResultNulls_ && rawValues_ && (numValues_ + 1) * sizeof(T) < rawSize_);

		anyNulls_ = true;
		bits::setBit(rawResultNulls_, numValues_);
		reinterpret_cast<T *>(rawValues_)[numValues_] = T();
		numValues_++;
	}

	template <typename T>
	inline void addValue(const T value) {
		// @lint-ignore-every HOWTOEVEN ConstantArgumentPassByValue
		static_assert(std::is_pod<T>::value, "General case of addValue is only for primitive types");
		DATALIB_DCHECK(rawValues_ && (numValues _ + 1) * sizeof(T) < rawSize_);
		reinterpret_cast<T *>(rawValues_)[numValues_] = value;
		numValues_++;
	}

	void dropResults(vector_size_t count) {
		outputRows_.resize(outputRows_.size() - count);
		numValues_ -= count;
	}

	common::ScanSpec *scanSpec() const {
		return scanSpec_;
	}

	auto readOffset() const {
		return readOffset_;
	}

	void setReadOffset(vector_size_t readOffset) {
		readOffset_ = readOffset;
	}

protected:
	static constexpr int8_t kNoValueSize = -1;

	template <typename T>
	void ensureValuesCapacity(vector_size_t numRows);

	void prepareNulls(vector_size_t numRows, bool needNulls);

	template <typename T>
	void filterNulls(RowSet rows, bool isNull, bool extractValues);

	template <typename T>
	void prepareRead(vector_size_t offset, RowSet rows, const uint64_t *incomingNulls);

	void setOutputRows(RowSet rows) {
		outputRows_.resize(rows.size());
		if (!rows.size()) {
			return;
		}
		memcpy(outputRows_.data(), &rows[0], rows.size() * sizeof(vector_size_t));
	}
	template <typename T, typename TVector>
	void getFlatValues(RowSet rows, VectorPtr *result);

	template <typename T, typename TVector>
	void compactScalarValues(RowSet rows);

	void addStringValue(folly::StringPiece value);

	// Specification of filters, value extraction, pruning etc. The
	// spec is assigned at construction and the contents may change at
	// run time based on adaptation. Owned by caller.
	common::ScanSpec *const scanSpec_;
	// Row number after last read row, relative to stripe start.
	vector_size_t readOffset_ = 0;
	// The rows to process in read(). References memory supplied by
	// caller. The values must remain live until the next call to read().
	RowSet inputRows_;
	// Rows passing the filter in readWithVisitor. Must stay
	// constant between consecutive calls to read().
	vector<vector_size_t> outputRows_;
	// The row number corresponding to each element in 'values_'
	vector<vector_size_t> valueRows_;
	// The set of all nulls in the range of read(). Created when first
	// needed and then reused. Not returned to callers.
	BufferPtr nullsInReadRange_;
	// Nulls buffer for readWithVisitor. Not set if no nulls. 'numValues'
	// is the index of the first non-set bit.
	BufferPtr resultNulls_;
	uint64_t *rawResultNulls_ = nullptr;
	// Buffer for gathering scalar values in readWithVisitor.
	BufferPtr values_;
	// Writable content in 'values'
	void *rawValues_ = nullptr;
	vector_size_t numValues_ = 0;
	// Size of fixed width value in 'rawValues'. For integers, values
	// are read at 64 bit width and can be compacted or extracted at a
	// different width.
	int8_t valueSize_ = kNoValueSize;
	// Buffers backing the StringViews in 'values' when reading strings.
	vector<BufferPtr> stringBuffers_;
	// Writable contents of 'stringBuffers_.back()'.
	char *rawStringBuffer_ = nullptr;
	// Total writable bytes in 'rawStringBuffer_'.
	int32_t rawStringSize_ = 0;
	// Number of written bytes in 'rawStringBuffer_'.
	uint32_t rawStringUsed_ = 0;

	// True if last read() added any nulls.
	bool anyNulls_ = false;
	// True if all values in scope for last read() are null.
	bool allNull_ = false;
};

struct ExtractValues {
	static constexpr bool kSkipNulls = false;

	bool acceptsNulls() const {
		return true;
	}

	template <typename V>
	void addValue(vector_size_t /*rowIndex*/, V /*value*/) {
	}
	void addNull(vector_size_t /*rowIndex*/) {
	}
};

class Filter {
protected:
	Filter(bool deterministic, bool nullAllowed, FilterKind kind)
	    : nullAllowed_(nullAllowed), deterministic_(deterministic), kind_(kind) {
	}

public:
	virtual ~Filter() = default;

	// Templates parametrized on filter need to know determinism at compile
	// time. If this is false, deterministic() will be consulted at
	// runtime.
	static constexpr bool deterministic = true;

	FilterKind kind() const {
		return kind_;
	}

	virtual unique_ptr<Filter> clone() const = 0;

	/**
	 * A filter becomes non-deterministic when applies to nested column,
	 * e.g. a[1] > 10 is non-deterministic because > 10 filter applies only to
	 * some positions, e.g. first entry in a set of entries that correspond to a
	 * single top-level position.
	 */
	virtual bool isDeterministic() const {
		return deterministic_;
	}

	/**
	 * When a filter applied to a nested column fails, the whole top-level
	 * position should fail. To enable this functionality, the filter keeps track
	 * of the boundaries of top-level positions and allows the caller to find out
	 * where the current top-level position started and how far it continues.
	 * @return number of positions from the start of the current top-level
	 * position up to the current position (excluding current position)
	 */
	virtual int getPrecedingPositionsToFail() const {
		return 0;
	}

	/**
	 * @return number of positions remaining until the end of the current
	 * top-level position
	 */
	virtual int getSucceedingPositionsToFail() const {
		return 0;
	}

	virtual bool testNull() const {
		return nullAllowed_;
	}

	/**
	 * Used to apply is [not] null filters to complex types, e.g.
	 * a[1] is null AND a[3] is not null, where a is an array(array(T)).
	 *
	 * In these case, the exact values are not known, but it is known whether they
	 * are null or not. Furthermore, for some positions only nulls are allowed
	 * (a[1] is null), for others only non-nulls (a[3] is not null), and for the
	 * rest both are allowed (a[2] and a[N], where N > 3).
	 */
	virtual bool testNonNull() const {
		DWIO_RAISE("not supported");
	}

	virtual bool testInt64(int64_t /* unused */) const {
		DWIO_RAISE("not supported");
	}

	virtual bool testDouble(double /* unused */) const {
		DWIO_RAISE("not supported");
	}

	virtual bool testFloat(float /* unused */) const {
		DWIO_RAISE("not supported");
	}

	virtual bool testBool(bool /* unused */) const {
		DWIO_RAISE("not supported");
	}

	virtual bool testBytes(const char * /* unused */, int32_t /* unused */) const {
		DWIO_RAISE("not supported");
	}

	/**
	 * Filters like string equality and IN, as well as conditions on cardinality
	 * of lists and maps can be at least partly decided by looking at lengths
	 * alone. If this is false, then no further checks are needed. If true,
	 * eventual filters on the data itself need to be evaluated.
	 */
	virtual bool testLength(int32_t /* unused */) const {
		DWIO_RAISE("not supported");
	}

protected:
	const bool nullAllowed_;

private:
	const bool deterministic_;
	const FilterKind kind_;
};

// Template parameter for controlling filtering and action on a set of rows.
template <typename T, typename TFilter, typename ExtractValues, bool isDense>
class ColumnVisitor {
public:
	using FilterType = TFilter;
	static constexpr bool dense = isDense;
	ColumnVisitor(TFilter &filter, SelectiveColumnReader *reader, const RowSet &rows, ExtractValues values)
	    : filter_(filter), reader_(reader), allowNulls_(!TFilter::deterministic || filter.testNull()), rows_(&rows[0]),
	      numRows_(rows.size()), rowIndex_(0), values_(values) {
	}

	bool allowNulls() {
		if (ExtractValues::kSkipNulls && TFilter::deterministic) {
			return false;
		}
		return allowNulls_ && values_.acceptsNulls();
	}

	vector_size_t start() {
		return isDense ? 0 : rowAt(0);
	}

	// Tests for a null value and processes it. If the value is not
	// null, returns 0 and has no effect. If the value is null, advances
	// to the next non-null value in 'rows_'. Returns the number of
	// values (not including nulls) to skip to get to the next non-null.
	// If there is no next non-null in 'rows_', sets 'atEnd'. If 'atEnd'
	// is set and a non-zero skip is returned, the caller must perform
	// the skip before returning.
	FOLLY_ALWAYS_INLINE vector_size_t checkAndSkipNulls(const uint64_t *nulls, vector_size_t &current, bool &atEnd) {
		auto testRow = currentRow();
		// Check that the caller and the visitor are in sync about current row.
		DATALIB_DCHECK(current == testRow);
		uint32_t nullIndex = testRow >> 6;
		uint64_t nullWord = nulls[nullIndex];
		if (!nullWord) {
			return 0;
		}
		uint8_t nullBit = testRow & 63;
		if ((nullWord & (1UL << nullBit)) == 0) {
			return 0;
		}
		// We have a null. We find the next non-null.
		if (++rowIndex_ >= numRows_) {
			atEnd = true;
			return 0;
		}
		auto rowOfNullWord = testRow - nullBit;
		if (isDense) {
			if (nullBit == 63) {
				nullBit = 0;
				rowOfNullWord += 64;
				nullWord = nulls[++nullIndex];
			} else {
				++nullBit;
				// set all the bits below the row to null.
				nullWord |= f4d::bits::lowMask(nullBit);
			}
			for (;;) {
				auto nextNonNull = count_trailing_zeros(~nullWord);
				if (rowOfNullWord + nextNonNull >= numRows_) {
					// Nulls all the way to the end.
					atEnd = true;
					return 0;
				}
				if (nextNonNull < 64) {
					DATALIB_CHECK(rowIndex_ <= rowOfNullWord + nextNonNull);
					rowIndex_ = rowOfNullWord + nextNonNull;
					current = currentRow();
					return 0;
				}
				rowOfNullWord += 64;
				nullWord = nulls[++nullIndex];
			}
		} else {
			// Sparse row numbers. We find the first non-null and count
			// how many non-nulls on rows not in 'rows_' we skipped.
			int32_t toSkip = 0;
			nullWord |= f4d::bits::lowMask(nullBit);
			for (;;) {
				testRow = currentRow();
				while (testRow >= rowOfNullWord + 64) {
					toSkip += __builtin_popcountll(~nullWord);
					nullWord = nulls[++nullIndex];
					rowOfNullWord += 64;
				}
				// testRow is inside nullWord. See if non-null.
				nullBit = testRow & 63;
				if ((nullWord & (1UL << nullBit)) == 0) {
					toSkip += __builtin_popcountll(~nullWord & f4d::bits::lowMask(nullBit));
					current = testRow;
					return toSkip;
				}
				if (++rowIndex_ >= numRows_) {
					// We end with a null. Add the non-nulls below the final null.
					toSkip += __builtin_popcountll(~nullWord & f4d::bits::lowMask(testRow - rowOfNullWord));
					atEnd = true;
					return toSkip;
				}
			}
		}
	}

	vector_size_t processNull(bool &atEnd) {
		vector_size_t previous = currentRow();
		if (filter_.testNull()) {
			filterPassedForNull();
		} else {
			filterFailed();
		}
		if (++rowIndex_ >= numRows_) {
			atEnd = true;
			return rows_[numRows_ - 1] - previous;
		}
		if (TFilter::deterministic && isDense) {
			return 0;
		}
		return currentRow() - previous - 1;
	}

	FOLLY_ALWAYS_INLINE vector_size_t process(T value, bool &atEnd) {
		if (!TFilter::deterministic) {
			auto previous = currentRow();
			if (common::applyFilter(filter_, value)) {
				filterPassed(value);
			} else {
				filterFailed();
			}
			if (++rowIndex_ >= numRows_) {
				atEnd = true;
				return rows_[numRows_ - 1] - previous;
			}
			return currentRow() - previous - 1;
		}
		// The filter passes or fails and we go to the next row if any.
		if (common::applyFilter(filter_, value)) {
			filterPassed(value);
		} else {
			filterFailed();
		}
		if (++rowIndex_ >= numRows_) {
			atEnd = true;
			return 0;
		}
		if (isDense) {
			return 0;
		}
		return currentRow() - rows_[rowIndex_ - 1] - 1;
	}

	inline vector_size_t rowAt(vector_size_t index) {
		if (isDense) {
			return index;
		}
		return rows_[index];
	}

	vector_size_t currentRow() {
		if (isDense) {
			return rowIndex_;
		}
		return rows_[rowIndex_];
	}

	vector_size_t numRows() {
		return numRows_;
	}

	void filterPassed(T value) {
		addResult(value);
		if (!std::is_same<TFilter, common::AlwaysTrue>::value) {
			addOutputRow(currentRow());
		}
	}

	inline void filterPassedForNull() {
		addNull();
		if (!std::is_same<TFilter, common::AlwaysTrue>::value) {
			addOutputRow(currentRow());
		}
	}

	FOLLY_ALWAYS_INLINE void filterFailed();
	inline void addResult(T value);
	inline void addNull();
	inline void addOutputRow(vector_size_t row);

protected:
	TFilter &filter_;
	SelectiveColumnReader *reader_;
	const bool allowNulls_;
	const vector_size_t *rows_;
	vector_size_t numRows_;
	vector_size_t rowIndex_;
	ExtractValues values_;
};

} // namespace duckdb
