#pragma once

#include "duckdb/storage/arena_allocator.hpp"

#include <type_traits>

namespace duckdb {

template<class T>
class ArenaLinkedList {
public:
	static_assert(std::is_trivially_destructible<T>::value, "T must be trivially destructible");

	ArenaLinkedList() = default;
	ArenaLinkedList(const ArenaLinkedList &) = delete;
	ArenaLinkedList &operator=(const ArenaLinkedList &) = delete;

	ArenaLinkedList(ArenaLinkedList &&other) noexcept : head(other.head), tail(other.tail) {
		other.head = nullptr;
		other.tail = nullptr;
	}

	ArenaLinkedList &operator=(ArenaLinkedList &&other) noexcept {
		if (this != &other) {
			head = other.head;
			tail = other.tail;
			other.head = nullptr;
			other.tail = nullptr;
		}
		return *this;
	}
public:
	void push_back(ArenaAllocator &arena, const T& value) {
		auto node = arena.Make<Node>(value);
		auto ptr = head ? &tail->next : &head;
		*ptr = node;
		tail = node;
	}

	template<class ...ARGS>
	void emplace_back(ArenaAllocator &arena, ARGS&&... args) {
		auto node = arena.Make<Node>(std::forward<ARGS>(args)...);
		auto ptr = head ? &tail->next : &head;
		*ptr = node;
		tail = node;
	}

	struct Iterator;
	struct ConstIterator;

	Iterator begin();
	Iterator end();
	ConstIterator begin() const;
	ConstIterator end() const;

private:
	struct Node {
		explicit Node(const T& value_p) : next(nullptr), value(value_p) {}
		Node *next;
		T value;
	};

	Node* head = nullptr;
	Node* tail = nullptr;
};


template<class T>
struct ArenaLinkedList<T>::Iterator {
	Node* node;

	explicit Iterator(Node* node_p) : node(node_p) {}

	T& operator*() {
		return node->value;
	}

	Iterator& operator++() {
		node = node->next;
		return *this;
	}

	bool operator!=(const Iterator& other) const {
		return node != other.node;
	}
};

template<class T>
struct ArenaLinkedList<T>::ConstIterator {
	const Node* node;

	explicit ConstIterator(const Node* node_p) : node(node_p) {}

	const T& operator*() const {
		return node->value;
	}

	ConstIterator& operator++() {
		node = node->next;
		return *this;
	}

	bool operator!=(const ConstIterator& other) const {
		return node != other.node;
	}
};

template<class T>
typename ArenaLinkedList<T>::Iterator ArenaLinkedList<T>::begin() {
	return Iterator(head);
}

template<class T>
typename ArenaLinkedList<T>::Iterator ArenaLinkedList<T>::end() {
	return Iterator(nullptr);
}

template<class T>
typename ArenaLinkedList<T>::ConstIterator ArenaLinkedList<T>::begin() const {
	return ConstIterator(head);
}

template<class T>
typename ArenaLinkedList<T>::ConstIterator ArenaLinkedList<T>::end() const {
	return ConstIterator(nullptr);
}

} // namespace duckdb