// Vendored from https://github.com/mapbox/variant at tag v1.1.5

#ifndef MAPBOX_UTIL_VARIANT_IO_HPP
#define MAPBOX_UTIL_VARIANT_IO_HPP

#include <iosfwd>

#include "variant.hpp"

namespace mapbox {
namespace util {

namespace detail {
// operator<< helper
template <typename Out>
class printer
{
public:
    explicit printer(Out& out)
        : out_(out) {}
    printer& operator=(printer const&) = delete;

    // visitor
    template <typename T>
    void operator()(T const& operand) const
    {
        out_ << operand;
    }

private:
    Out& out_;
};
}

// operator<<
template <typename CharT, typename Traits, typename... Types>
VARIANT_INLINE std::basic_ostream<CharT, Traits>&
operator<<(std::basic_ostream<CharT, Traits>& out, variant<Types...> const& rhs)
{
    detail::printer<std::basic_ostream<CharT, Traits>> visitor(out);
    apply_visitor(visitor, rhs);
    return out;
}
} // namespace util
} // namespace mapbox

#endif // MAPBOX_UTIL_VARIANT_IO_HPP
