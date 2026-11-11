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
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <queue>
#include <set>
#include <string>
#include <utility>
#include <vector>
#include "cRaftSafetyObserver.hpp"
#include "cSimulationRandom.hpp"
#include "sSimulatedNode.hpp"
#include "sSimulationCounters.hpp"
#include "sSimulationEvent.hpp"
#include "sSimulationOptions.hpp"
#include "sSimulationTraceEvent.hpp"
#include "stepraft/cInMemoryRaftLog.hpp"
#include "stepraft/cRaftNode.hpp"

namespace stepraft {

// Deterministic discrete-event executor for a cluster of RaftNode cores.
// Virtual time is ticks; the only source of nondeterminism is the PRNG
// seeded from the options, so a seed and the scenario's calls replay a
// run exactly. The simulator executes every Ready by the contract, and
// no more: messages of a Ready that demands a sync wait for the disk,
// others leave at once while the write completes later. A crash keeps
// only the durable part of a node's log and hard state.
class RaftSimulator {
 public:
  using Invariant =
      std::function<std::optional<std::string>(const RaftSimulator &)>;

  explicit RaftSimulator(SimulationOptions options);
  RaftSimulator(const RaftSimulator &) = delete;
  RaftSimulator &operator=(const RaftSimulator &) = delete;

  // Process every event due up to Now() + ticks. Returns false as soon
  // as an invariant or the executor contract is violated.
  bool RunFor(std::uint64_t ticks);

  // Scenario actions, applied at the current virtual time.
  void Send(const RaftMessage &message);
  bool Propose(std::uint32_t node, std::vector<std::uint8_t> payload);
  void TransferLeadership(std::uint32_t node, std::uint32_t successor);
  void Crash(std::uint32_t node);
  void Restart(std::uint32_t node);
  // Crash the node and give it new, empty storage: back up, it is the
  // same node and another voter.
  void Wipe(std::uint32_t node);
  // A node that is down writes what makes it the cluster's only voter, as
  // the command on the survivor does after the majority is lost for good;
  // a Restart() starts it so.
  void ContinueAlone(std::uint32_t node);
  RaftMembershipOutcome ChangeMembership(std::uint32_t node,
                                         const RaftMembershipChange &change);
  // The node learns the cluster's members anew; it keeps them over a restart.
  void SetMembers(std::uint32_t node, std::vector<std::uint32_t> members);
  std::uint64_t StorageOf(std::uint32_t node) const;
  // The node's log forgets its entries up to `index`, as eviction does;
  // only committed ones.
  bool Evict(std::uint32_t node, std::uint64_t index);
  // Nodes in different groups cannot reach each other; a member left
  // out of every group is isolated. Messages in flight across a new cut
  // are lost on arrival.
  void Partition(const std::vector<std::vector<std::uint32_t>> &groups);
  // Cut or restore the link between two nodes, both directions, on top
  // of any partition. Heal restores everything.
  void Cut(std::uint32_t a, std::uint32_t b);
  void Mend(std::uint32_t a, std::uint32_t b);
  void Heal();
  // Heal the network and stop random drops, duplicates and crashes;
  // delays stay. For checking that the cluster settles afterwards.
  void StopFaults();
  // Checked after every event, after the built-in safety checks.
  void AddInvariant(std::string name, Invariant check);

  std::uint64_t Now() const { return now_; }
  std::uint64_t Seed() const { return options_.seed; }
  const std::vector<std::uint32_t> &Members() const { return options_.members; }
  bool IsUp(std::uint32_t node) const;
  // Null while the node is down.
  const RaftNode *Core(std::uint32_t node) const;
  const InMemoryRaftLog &Log(std::uint32_t node) const;
  const SimulationCounters &Counters() const { return counters_; }
  const std::vector<SimulationTraceEvent> &Trace() const { return trace_; }
  // Hash over every recorded event, kept with or without the trace.
  std::uint64_t Digest() const { return digest_; }
  const std::optional<std::string> &Violation() const { return violation_; }
  // Seed, step, the violation and the tail of the trace: enough to
  // replay the run and read what led to it.
  std::string Report(std::size_t traceTail = 40) const;

 private:
  SimulatedNode &At(std::uint32_t node);
  const SimulatedNode &At(std::uint32_t node) const;
  void Schedule(SimulationEvent event);
  void Process(const SimulationEvent &event);
  void OnTick(std::uint32_t node);
  void OnDiskDone(std::uint32_t node);
  void Pump(std::uint32_t node);
  // After an Advance: take the next Ready now, or after the executor lag.
  void Advanced(std::uint32_t node);
  void Execute(std::uint32_t node, RaftReady ready);
  void SendAll(std::vector<RaftMessage> messages);
  // With a trace writer: the core reports its sends to `node.sent`.
  void Observe(SimulatedNode &node);
  std::vector<RaftMessage> TakeSent(SimulatedNode &node);
  bool Linked(std::uint32_t a, std::uint32_t b) const;
  std::uint32_t NodesDown() const;
  void Record(SimulationEventKind kind, std::uint32_t node,
              const RaftMessage *message = nullptr);
  void CheckInvariants();
  void Fail(std::string what);

  // Member i starts on storage FIRST_STORAGE + i; every wipe takes the
  // next unused one.
  static constexpr std::uint64_t FIRST_STORAGE = 100;
  SimulationOptions options_;
  SimulationRandom random_;
  // Same order as options_.members; pointers keep each log in place.
  std::vector<std::unique_ptr<SimulatedNode>> nodes_;
  std::uint64_t nextStorage_ = FIRST_STORAGE;
  std::priority_queue<SimulationEvent, std::vector<SimulationEvent>,
                      std::greater<>>
      queue_;
  std::uint64_t now_ = 0;
  std::uint64_t sequence_ = 0;
  // node -> partition group; empty when the network is whole.
  std::map<std::uint32_t, std::uint32_t> group_;
  // Cut links, stored as (smaller id, larger id).
  std::set<std::pair<std::uint32_t, std::uint32_t>> cuts_;
  std::vector<std::pair<std::string, Invariant>> invariants_;
  RaftSafetyObserver safety_;
  SimulationCounters counters_;
  std::vector<SimulationTraceEvent> trace_;
  std::uint64_t digest_ = 14695981039346656037ull;
  std::optional<std::string> violation_;
};

}  // namespace stepraft
