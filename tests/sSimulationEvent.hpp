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
#include "eSimulationEventKind.hpp"
#include "stepraft/sRaftMessage.hpp"

namespace stepraft {

struct SimulationEvent {
  std::uint64_t time = 0;
  // Events due at the same time run in the order they were scheduled.
  std::uint64_t sequence = 0;
  SimulationEventKind kind = SimulationEventKind::Tick;
  std::uint32_t node = 0;
  // Tick and DiskDone of a node's earlier life (before a crash) are void.
  std::uint64_t incarnation = 0;
  RaftMessage message;  // Deliver only

  // Min-heap order for std::priority_queue with std::greater.
  bool operator>(const SimulationEvent &other) const {
    return time != other.time ? time > other.time : sequence > other.sequence;
  }
};

}  // namespace stepraft
