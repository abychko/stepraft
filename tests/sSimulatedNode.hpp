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
#include <deque>
#include <memory>
#include "sSimulatedWrite.hpp"
#include "stepraft/cInMemoryRaftLog.hpp"
#include "stepraft/cRaftNode.hpp"
#include "stepraft/sRaftConfig.hpp"
#include "stepraft/sRaftMessage.hpp"

namespace stepraft {

// Must not move once `core` exists: the core keeps a reference to `log`.
struct SimulatedNode {
  RaftConfig config;
  // Outlives crashes, but only its durable part does.
  InMemoryRaftLog log;
  std::unique_ptr<RaftNode> core;  // null while down
  std::uint64_t incarnation = 0;
  // Taken Readies awaiting their disk completion before Advance.
  std::size_t awaiting = 0;
  // The executor is away after an Advance; Readies wait for it.
  bool executorAway = false;
  // Pending disk completions, oldest first, as a log fsync orders them.
  std::deque<SimulatedWrite> writes;
  std::uint64_t lastDiskDone = 0;
  // Commit index up to which the durable quorum was checked.
  std::uint64_t checkedCommit = 0;
  // With a trace writer: what the core sent since its last traced call.
  std::vector<RaftMessage> sent;
};

}  // namespace stepraft
