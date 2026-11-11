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

#include "stepraft/cRaftMembership.hpp"

namespace stepraft {

// The node state that must survive a restart. Losing (term, votedFor)
// permits a double vote within one term, so the executor persists this
// before any message that the state change authorizes is sent.
struct RaftHardState {
  std::uint64_t term = 0;
  std::optional<std::uint32_t> votedFor;
  std::uint64_t commitIndex = 0;
  // The membership committed by then and the index of its entry: it
  // outlives the entry's eviction. Empty: the one the config starts with.
  std::vector<RaftVoter> voters;
  std::uint64_t votersIndex = 0;
  std::vector<std::uint8_t> votersContext;

  bool operator==(const RaftHardState &) const = default;
};

}  // namespace stepraft
