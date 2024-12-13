#pragma once
#include <algorithm>
#include <rapidfuzz/details/Range.hpp>
#include <rapidfuzz/details/type_traits.hpp>

namespace duckdb_rapidfuzz::detail {

template <typename InputIt>
class SplittedSentenceView {
public:
    using CharT = iter_value_t<InputIt>;

    SplittedSentenceView(RangeVec<InputIt> sentence) noexcept(
        std::is_nothrow_move_constructible_v<RangeVec<InputIt>>)
        : m_sentence(std::move(sentence))
    {}

    size_t dedupe();
    size_t size() const;

    size_t length() const
    {
        return size();
    }

    bool empty() const
    {
        return m_sentence.empty();
    }

    size_t word_count() const
    {
        return m_sentence.size();
    }

    std::vector<CharT> join() const;

    const RangeVec<InputIt>& words() const
    {
        return m_sentence;
    }

private:
    RangeVec<InputIt> m_sentence;
};

template <typename InputIt>
size_t SplittedSentenceView<InputIt>::dedupe()
{
    size_t old_word_count = word_count();
    m_sentence.erase(std::unique(m_sentence.begin(), m_sentence.end()), m_sentence.end());
    return old_word_count - word_count();
}

template <typename InputIt>
size_t SplittedSentenceView<InputIt>::size() const
{
    if (m_sentence.empty()) return 0;

    // there is a whitespace between each word
    size_t result = m_sentence.size() - 1;
    for (const auto& word : m_sentence) {
        result += static_cast<size_t>(std::distance(word.begin(), word.end()));
    }

    return result;
}

template <typename InputIt>
auto SplittedSentenceView<InputIt>::join() const -> std::vector<CharT>
{
    if (m_sentence.empty()) {
        return std::vector<CharT>();
    }

    auto sentence_iter = m_sentence.begin();
    std::vector<CharT> joined(sentence_iter->begin(), sentence_iter->end());
    ++sentence_iter;
    for (; sentence_iter != m_sentence.end(); ++sentence_iter) {
        joined.push_back(0x20);
        joined.insert(joined.end(), sentence_iter->begin(), sentence_iter->end());
    }
    return joined;
}

} // namespace duckdb_rapidfuzz::detail
