namespace duckdb {

template <typename T>
class weak_ptr {
private:
	template <class U>
	friend class shared_ptr;
	std::weak_ptr<T> internal;

public:
	// Constructors
	weak_ptr() : internal() {
	}
	// template <class U, std::enable_if<__compatible_with<U, T>::value, int> = 0>
	template <class U>
	weak_ptr(const shared_ptr<U> &ptr) : internal(ptr.internal) {
	}
	weak_ptr(const weak_ptr &other) : internal(other.internal) {
	}

	// Destructor
	~weak_ptr() = default;

	// Assignment operators
	weak_ptr &operator=(const weak_ptr &other) {
		internal = other.internal;
		return *this;
	}

	template <class U, std::enable_if<__compatible_with<U, T>::value, int> = 0>
	weak_ptr &operator=(const shared_ptr<U> &ptr) {
		internal = ptr;
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

	shared_ptr<T> lock() const {
		return internal.lock();
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
