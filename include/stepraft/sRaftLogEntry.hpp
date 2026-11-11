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
#include <vector>

namespace stepraft {

// A kind is a field, not a position in the entry layout, so new kinds
// extend the set without breaking stored logs.
enum class RaftEntryKind : std::uint8_t {
  // Opaque source bytes (one replicated unit). The core never interprets
  // them; external coordinates inside belong to the log implementation.
  Payload = 0,
  // Service entry a new leader appends at the start of its term. Carries
  // no payload and never reaches downstream readers.
  NoOp = 1,
  // The voting members from this entry on (RaftMembership::Encode); the
  // core reads it, downstream readers never see it.
  Membership = 2,
};

struct RaftLogEntry {
  std::uint64_t term = 0;
  std::uint64_t index = 0;
  RaftEntryKind kind = RaftEntryKind::Payload;
  std::vector<std::uint8_t> payload;

  bool operator==(const RaftLogEntry &) const = default;
};

}  // namespace stepraft
