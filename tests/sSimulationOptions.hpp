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
#include "stepraft/sRaftConfig.hpp"

namespace stepraft {

class RaftTraceWriter;

// Everything a run depends on besides the scenario's own calls: the same
// options and the same calls replay the same run.
struct SimulationOptions {
  std::uint64_t seed = 1;
  std::vector<std::uint32_t> members{1, 2, 3};
  // Template for every node; selfId and members are filled per node.
  RaftConfig raft;

  // Network: each message is dropped, duplicated and delayed on its own;
  // reordering follows from the delay spread.
  std::uint32_t dropPpm = 0;
  std::uint32_t duplicatePpm = 0;
  std::uint32_t delayMinTicks = 1;
  std::uint32_t delayMaxTicks = 1;

  // Disk: time from "written" to "durable". Completions keep write order.
  std::uint32_t diskDelayMinTicks = 1;
  std::uint32_t diskDelayMaxTicks = 1;

  // Up to this many ticks between acknowledging one Ready and taking the
  // next; the node keeps receiving events meanwhile, as it would under an
  // executor with its own loop.
  std::uint32_t executorLagMaxTicks = 0;

  // Up to this many extra ticks between two ticks of one node.
  std::uint32_t tickJitterTicks = 0;

  // Random crashes are drawn at each node tick; no more than maxNodesDown
  // nodes are down at once, and a crashed node restarts after a delay.
  std::uint32_t crashPpm = 0;
  std::uint32_t maxNodesDown = 1;
  std::uint32_t restartDelayMinTicks = 1;
  std::uint32_t restartDelayMaxTicks = 1;

  // The members the cluster starts voting with, each on its storage; empty:
  // all. A wiped node comes back as another voter.
  std::vector<std::uint32_t> voters;
  // The nodes that start knowing only these as members, as a cluster that
  // others join later; a node not among them knows all. Empty: all know
  // all.
  std::vector<std::uint32_t> known;

  // The digest is kept either way; the trace costs memory per event.
  bool trace = true;
  // Every call into a core, as NDJSON for trace validation; not owned.
  RaftTraceWriter *traceWriter = nullptr;
};

}  // namespace stepraft
