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
#include "stepraft/sRaftHardState.hpp"
#include "stepraft/sRaftMessage.hpp"

namespace stepraft {

// One executed Ready waiting for its disk completion.
struct SimulatedWrite {
  // The log's write sequence right after this Ready's writes.
  std::uint64_t logSequence = 0;
  std::optional<RaftHardState> hardState;
  // Held back until durable when the Ready demanded a sync.
  std::vector<RaftMessage> messages;
  bool advanceOnDone = false;
};

}  // namespace stepraft
