/* Copyright (c) 2026, Alexey Bychko.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of stepraft hereby grant you an
   additional permission to link the program and your derivative works
   with the separately licensed software that they have either included
   with the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#pragma once
#include <cstdint>
#include <optional>
#include <vector>
#include "stepraft/sRaftLogEntry.hpp"

namespace stepraft {

// Read-only view of the replicated log. The core only ever reads;
// writes happen when the executor applies a RaftReady. The interface is
// independent of the representation: where terms, indexes and payload
// bytes live (mirrored source files + side metadata, own container,
// in-band) is the implementation's business.
class RaftLogView {
 public:
  virtual ~RaftLogView() = default;

  // Index after the log's base: 1 on a fresh log, growing when a prefix
  // is evicted or the log restarts after a base. Zero means a fresh,
  // empty log.
  virtual std::uint64_t FirstIndex() const = 0;
  // Index of the last appended entry, or the base when nothing follows it.
  virtual std::uint64_t LastIndex() const = 0;
  // Term of the entry at `index`, or nullopt when the index is outside
  // [FirstIndex - 1, LastIndex]. FirstIndex - 1 is the base, whose term
  // the log keeps; index 0 is the empty-log sentinel: term 0.
  virtual std::optional<std::uint64_t> TermAt(std::uint64_t index) const = 0;
  // Entries [first, last] inclusive, cut to at most `maxBytes` of
  // payload (at least one entry when the range is non-empty).
  virtual std::vector<RaftLogEntry> Read(std::uint64_t first,
                                         std::uint64_t last,
                                         std::uint64_t maxBytes) const = 0;
  // The membership entries past `after`, oldest first: what the core
  // reads at start instead of the whole log.
  virtual std::vector<RaftLogEntry> MembershipEntries(
      std::uint64_t after) const = 0;
};

}  // namespace stepraft
