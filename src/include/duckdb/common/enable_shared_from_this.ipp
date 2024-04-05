namespace duckdb {

template <class _Tp>
class enable_shared_from_this {
	mutable weak_ptr<_Tp> __weak_this_;

protected:
	constexpr enable_shared_from_this() noexcept {
	}
	enable_shared_from_this(enable_shared_from_this const &) noexcept {
	}
	enable_shared_from_this &operator=(enable_shared_from_this const &) noexcept {
		return *this;
	}
	~enable_shared_from_this() {
	}

public:
	shared_ptr<_Tp> shared_from_this() {
		return shared_ptr<_Tp>(__weak_this_);
	}
	shared_ptr<_Tp const> shared_from_this() const {
		return shared_ptr<const _Tp>(__weak_this_);
	}

#if _LIBCPP_STD_VER >= 17
	weak_ptr<_Tp> weak_from_this() noexcept {
		return __weak_this_;
	}

	weak_ptr<const _Tp> weak_from_this() const noexcept {
		return __weak_this_;
	}
#endif // _LIBCPP_STD_VER >= 17

	template <class _Up>
	friend class shared_ptr;
};

} // namespace duckdb
