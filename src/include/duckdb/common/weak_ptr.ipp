namespace duckdb {

template <typename T, bool SAFE>
class weak_ptr {
public:
	using original = std::weak_ptr<T>;
	using element_type = typename original::element_type;

private:
	template <class U, bool SAFE_P>
	friend class shared_ptr;

private:
	original internal;

public:
	// Constructors
	weak_ptr() : internal() {
	}

	template <class U>
	weak_ptr(shared_ptr<U, SAFE> const &ptr,
	         typename std::enable_if<compatible_with_t<U, T>::value, int>::type = 0) noexcept
	    : internal(ptr.internal) {
	}
	weak_ptr(weak_ptr const &other) noexcept : internal(other.internal) {
	}
	template <class U>
	weak_ptr(weak_ptr<U> const &ptr, typename std::enable_if<compatible_with_t<U, T>::value, int>::type = 0) noexcept
	    : internal(ptr.internal) {
	}
	weak_ptr(weak_ptr &&ptr) noexcept : internal(std::move(ptr.internal)) {
	}
	template <class U>
	weak_ptr(weak_ptr<U> &&ptr, typename std::enable_if<compatible_with_t<U, T>::value, int>::type = 0) noexcept
	    : internal(std::move(ptr.internal)) {
	}
	// Destructor
	~weak_ptr() = default;

	// Assignment operators
	weak_ptr &operator=(const weak_ptr &other) {
		internal = other.internal;
		return *this;
	}

	template <class U, typename std::enable_if<compatible_with_t<U, T>::value, int>::type = 0>
	weak_ptr &operator=(const shared_ptr<U, SAFE> &ptr) {
		internal = ptr.internal;
		return *this;
	}

	// Modifiers
	void reset() {
		internal.reset();
	}

	// Observers
	long use_count() const {
		return internal.use_count();
	}

	bool expired() const {
		return internal.expired();
	}

	shared_ptr<T, SAFE> lock() const {
		return shared_ptr<T, SAFE>(internal.lock());
	}

	// Relational operators
	template <typename U>
	bool operator==(const weak_ptr<U> &other) const noexcept {
		return internal == other.internal;
	}

	template <typename U>
	bool operator!=(const weak_ptr<U> &other) const noexcept {
		return internal != other.internal;
	}

	template <typename U>
	bool operator<(const weak_ptr<U> &other) const noexcept {
		return internal < other.internal;
	}

	template <typename U>
	bool operator<=(const weak_ptr<U> &other) const noexcept {
		return internal <= other.internal;
	}

	template <typename U>
	bool operator>(const weak_ptr<U> &other) const noexcept {
		return internal > other.internal;
	}

	template <typename U>
	bool operator>=(const weak_ptr<U> &other) const noexcept {
		return internal >= other.internal;
	}
};

} // namespace duckdb
