
#pragma once

#include <string>

class Printable {
  public:
	virtual ~Printable(){};

	/** @brief Get the info about the object. */
	virtual std::string ToString() const = 0;

	void Print();
};