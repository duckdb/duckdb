from io import StringIO, TextIOBase
from typing import Union

"""
BSD 3-Clause License

Copyright (c) 2008-2011, AQR Capital Management, LLC, Lambda Foundry, Inc. and PyData Development Team
All rights reserved.

Copyright (c) 2011-2022, Open source contributors.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of the copyright holder nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

class BytesIOWrapper:
	# Wrapper that wraps a StringIO buffer and reads bytes from it
	# Created for compat with pyarrow read_csv
	def __init__(self, buffer: Union[StringIO, TextIOBase], encoding: str = "utf-8") -> None:
		self.buffer = buffer
		self.encoding = encoding
		# Because a character can be represented by more than 1 byte,
		# it is possible that reading will produce more bytes than n
		# We store the extra bytes in this overflow variable, and append the
		# overflow to the front of the bytestring the next time reading is performed
		self.overflow = b""

	def __getattr__(self, attr: str):
		return getattr(self.buffer, attr)

	def read(self, n: Union[int, None] = -1) -> bytes:
		assert self.buffer is not None
		bytestring = self.buffer.read(n).encode(self.encoding)
		#When n=-1/n greater than remaining bytes: Read entire file/rest of file
		combined_bytestring = self.overflow + bytestring
		if n is None or n < 0 or n >= len(combined_bytestring):
			self.overflow = b""
			return combined_bytestring
		else:
			to_return = combined_bytestring[:n]
			self.overflow = combined_bytestring[n:]
			return to_return
