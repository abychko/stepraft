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
#include <algorithm>
#include <cstdint>
#include <initializer_list>
#include <optional>
#include <string>
#include <vector>
#include "cRaftSimulator.hpp"
#include "cSimulationRandom.hpp"

// Building blocks shared by the scenario tests.
namespace stepraft::scenario {

inline SimulationOptions Ring(std::uint64_t seed, std::uint32_t nodes = 3) {
  SimulationOptions options;
  options.seed = seed;
  options.members.clear();
  for (std::uint32_t id = 1; id <= nodes; ++id) options.members.push_back(id);
  options.maxNodesDown = (nodes - 1) / 2;
  return options;
}

// Message faults, clock drift and crashes of at most a minority.
inline SimulationOptions Chaos(std::uint64_t seed, std::uint32_t nodes) {
  SimulationOptions options = Ring(seed, nodes);
  options.dropPpm = 50'000;
  options.duplicatePpm = 20'000;
  options.delayMaxTicks = 5;
  options.diskDelayMaxTicks = 4;
  options.tickJitterTicks = 1;
  options.executorLagMaxTicks = 2;
  options.crashPpm = 2'000;
  options.restartDelayMinTicks = 5;
  options.restartDelayMaxTicks = 40;
  options.trace = false;
  return options;
}

// The leader every running node agrees on, all in one term.
inline std::optional<std::uint32_t> AgreedLeader(const RaftSimulator &sim) {
  std::optional<std::uint32_t> leader;
  std::optional<std::uint64_t> term;
  for (const std::uint32_t id : sim.Members()) {
    const RaftNode *core = sim.Core(id);
    if (!core) continue;
    if (core->LeaderId() == 0) return std::nullopt;
    if (leader && (*leader != core->LeaderId() || *term != core->Term()))
      return std::nullopt;
    leader = core->LeaderId();
    term = core->Term();
  }
  if (!leader || !sim.IsUp(*leader) ||
      sim.Core(*leader)->Role() != RaftRole::Leader)
    return std::nullopt;
  return leader;
}

inline std::optional<std::uint32_t> RunUntilLeader(RaftSimulator &sim,
                                                   std::uint64_t maxTicks) {
  for (std::uint64_t tick = 0; tick < maxTicks; ++tick) {
    if (const auto leader = AgreedLeader(sim)) return leader;
    if (!sim.RunFor(1)) return std::nullopt;
  }
  return AgreedLeader(sim);
}

inline std::vector<std::uint32_t> AllBut(const RaftSimulator &sim,
                                         std::uint32_t excluded) {
  std::vector<std::uint32_t> rest;
  for (const std::uint32_t id : sim.Members())
    if (id != excluded) rest.push_back(id);
  return rest;
}

inline std::uint32_t OtherThan(const RaftSimulator &sim,
                               std::initializer_list<std::uint32_t> excluded) {
  for (const std::uint32_t id : sim.Members())
    if (std::find(excluded.begin(), excluded.end(), id) == excluded.end())
      return id;
  return 0;
}

inline std::vector<std::uint8_t> Payload(std::uint64_t value) {
  std::vector<std::uint8_t> bytes;
  for (int shift = 0; shift < 64; shift += 8)
    bytes.push_back(static_cast<std::uint8_t>(value >> shift));
  return bytes;
}

// Every so often: heal, cut one link, or isolate one node. Seeded from
// the run's own seed, so the whole run still replays.
inline void ShakeNetwork(RaftSimulator &sim, SimulationRandom &random) {
  const auto &members = sim.Members();
  const auto pick = [&] { return members[random.Below(members.size())]; };
  switch (random.Below(3)) {
    case 0:
      sim.Heal();
      break;
    case 1:
      sim.Cut(pick(), pick());
      break;
    default: {
      const std::uint32_t loner = pick();
      sim.Partition({{loner}, AllBut(sim, loner)});
      break;
    }
  }
}

// Log matching over every pair of running nodes: where two logs hold an
// entry with the same index and term, they agree on everything before it.
inline std::optional<std::string> LogMatchingViolation(
    const RaftSimulator &sim) {
  for (const std::uint32_t a : sim.Members()) {
    for (const std::uint32_t b : sim.Members()) {
      if (a >= b || !sim.IsUp(a) || !sim.IsUp(b)) continue;
      const RaftNode &left = *sim.Core(a);
      const RaftNode &right = *sim.Core(b);
      // Below the later base, one of them holds nothing.
      const std::uint64_t floor =
          std::max(left.FirstIndex(), right.FirstIndex()) - 1;
      std::uint64_t index = std::min(left.LastIndex(), right.LastIndex());
      if (index < floor) continue;
      while (index > floor && left.TermAt(index) != right.TermAt(index))
        --index;
      if (left.TermAt(index) != right.TermAt(index)) continue;
      for (std::uint64_t below = index; below > floor; --below)
        if (left.TermAt(below - 1) != right.TermAt(below - 1))
          return "log matching: nodes " + std::to_string(a) + " and " +
                 std::to_string(b) + " share entry " + std::to_string(index) +
                 " but differ at " + std::to_string(below - 1);
    }
  }
  return std::nullopt;
}

// How running nodes' logs differ: last index, or the term at an index
// both hold. nullopt when they agree.
inline std::optional<std::string> LogDifference(const RaftSimulator &sim) {
  std::uint32_t referenceId = 0;
  const RaftNode *reference = nullptr;
  for (const std::uint32_t id : sim.Members()) {
    const RaftNode *core = sim.Core(id);
    if (!core) continue;
    if (!reference) {
      referenceId = id;
      reference = core;
      continue;
    }
    const std::string pair =
        "nodes " + std::to_string(referenceId) + " and " + std::to_string(id);
    if (core->LastIndex() != reference->LastIndex())
      return pair + " end at " + std::to_string(reference->LastIndex()) +
             " and " + std::to_string(core->LastIndex());
    const std::uint64_t floor =
        std::max(core->FirstIndex(), reference->FirstIndex()) - 1;
    for (std::uint64_t index = std::max<std::uint64_t>(floor, 1);
         index <= core->LastIndex(); ++index)
      if (core->TermAt(index) != reference->TermAt(index))
        return pair + " differ at " + std::to_string(index) + " (first " +
               std::to_string(reference->FirstIndex()) + " and " +
               std::to_string(core->FirstIndex()) + ")";
  }
  return std::nullopt;
}

// Each node's role, term, log, storage and membership, one per line.
inline std::string DescribeNodes(const RaftSimulator &sim) {
  std::string out;
  for (const std::uint32_t id : sim.Members()) {
    const RaftNode *core = sim.Core(id);
    out += "node " + std::to_string(id) + ": ";
    if (!core) {
      out += "down\n";
      continue;
    }
    out += std::string(core->Role() == RaftRole::Leader      ? "leader"
                       : core->Role() == RaftRole::Candidate ? "candidate"
                                                             : "follower") +
           " term " + std::to_string(core->Term()) + " log " +
           std::to_string(core->FirstIndex()) + ".." +
           std::to_string(core->LastIndex()) + " commit " +
           std::to_string(core->CommitIndex()) + " storage " +
           std::to_string(sim.StorageOf(id)) + " voters";
    for (const RaftVoter &voter : core->Voters())
      out += " " + std::to_string(voter.id) + "@" +
             std::to_string(voter.storageId);
    out += "\n";
  }
  return out;
}

inline bool LogsIdentical(const RaftSimulator &sim) {
  return !LogDifference(sim);
}

}  // namespace stepraft::scenario
