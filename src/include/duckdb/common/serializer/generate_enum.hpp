template<class T>
T EnumFromString(const char* value);

template<class T>
const char* StringFromEnum(T value);

#define FROM_STRING_NAME_IMPL(name) name##FromString
#define FROM_STRING_NAME(name) FROM_STRING_NAME_IMPL(name)

#define TO_STRING_NAME_IMPL(name) name##ToString
#define TO_STRING_NAME(name) TO_STRING_NAME_IMPL(name)

enum class ENUM_NAME : ENUM_TYPE {
#define MEMBER(name, val) name = val,
	ENUM_MEMBERS
#undef MEMBER
};

template<>
const char* StringFromEnum(ENUM_NAME value) {
#define MEMBER(name, val) case ENUM_NAME::name: return #name;
	switch (value) {
		ENUM_MEMBERS
	default: throw InternalException("Unreachable");
	}
#undef MEMBER
}

template<>
ENUM_NAME EnumFromString(const char* value) {
#define MEMBER(name, val) else if (strcmp(value, #name) == 0) { return ENUM_NAME::name; }
	if(false) {}
	ENUM_MEMBERS
	else { throw InternalException("Unreachable"); }
#undef MEMBER
}

#undef TO_STRING_NAME_IMPL
#undef TO_STRING_NAME

#undef FROM_STRING_NAME_IMPL
#undef FROM_STRING_NAME