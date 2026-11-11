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
#include <string>
#include "eSimulationEventKind.hpp"
#include "stepraft/sRaftMessage.hpp"

namespace stepraft {

struct SimulationTraceEvent {
  std::uint64_t time = 0;
  SimulationEventKind kind = SimulationEventKind::Tick;
  std::uint32_t node = 0;
  // Deliver, Drop and Duplicate: the message; zero otherwise.
  std::uint32_t from = 0;
  RaftMessageType type = RaftMessageType::AppendEntries;
  std::uint64_t term = 0;

  bool operator==(const SimulationTraceEvent &) const = default;

  std::string ToString() const {
    std::string line = "t=" + std::to_string(time) + " ";
    switch (kind) {
      case SimulationEventKind::Tick:
        return line + "tick " + std::to_string(node);
      case SimulationEventKind::DiskDone:
        return line + "disk-done " + std::to_string(node);
      case SimulationEventKind::Crash:
        return line + "crash " + std::to_string(node);
      case SimulationEventKind::Restart:
        return line + "restart " + std::to_string(node);
      case SimulationEventKind::Resume:
        return line + "resume " + std::to_string(node);
      case SimulationEventKind::Wipe:
        return line + "wipe " + std::to_string(node);
      case SimulationEventKind::Deliver:
        line += "deliver ";
        break;
      case SimulationEventKind::Drop:
        line += "drop ";
        break;
      case SimulationEventKind::Duplicate:
        line += "duplicate ";
        break;
    }
    return line + std::to_string(from) + "->" + std::to_string(node) +
           " type=" + std::to_string(static_cast<int>(type)) +
           " term=" + std::to_string(term);
  }
};

}  // namespace stepraft
