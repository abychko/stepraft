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

#include "cRaftSimulator.hpp"

#include <algorithm>
#include <cassert>
#include <sstream>
#include "cRaftTraceWriter.hpp"
#include "stepraft/hRaftSoleVoter.hpp"

namespace stepraft {

RaftSimulator::RaftSimulator(SimulationOptions options)
    : options_(std::move(options)), random_(options_.seed) {
  if (options_.traceWriter) options_.traceWriter->Begin(options_.members);
  for (const std::uint32_t id : options_.members) {
    auto node = std::make_unique<SimulatedNode>();
    node->config = options_.raft;
    node->config.selfId = id;
    node->config.members =
        std::find(options_.known.begin(), options_.known.end(), id) ==
                options_.known.end()
            ? options_.members
            : options_.known;
    node->config.randomSeed = random_.Next();
    node->config.storageId = nextStorage_++;
    for (std::size_t i = 0; i < options_.members.size(); ++i)
      if (options_.voters.empty() ||
          std::find(options_.voters.begin(), options_.voters.end(),
                    options_.members[i]) != options_.voters.end())
        if (std::find(node->config.members.begin(), node->config.members.end(),
                      options_.members[i]) != node->config.members.end())
          node->config.voters.push_back(
              {options_.members[i], FIRST_STORAGE + i});
    assert(node->config.Valid());
    node->core = std::make_unique<RaftNode>(node->config, node->log,
                                            node->log.HardState());
    Observe(*node);
    nodes_.push_back(std::move(node));
  }
  for (const std::uint32_t id : options_.members) {
    const std::uint64_t jitter =
        options_.tickJitterTicks ? random_.Below(options_.tickJitterTicks + 1)
                                 : 0;
    Schedule({1 + jitter, 0, SimulationEventKind::Tick, id, 0, {}});
  }
}

SimulatedNode &RaftSimulator::At(std::uint32_t node) {
  return const_cast<SimulatedNode &>(std::as_const(*this).At(node));
}

const SimulatedNode &RaftSimulator::At(std::uint32_t node) const {
  for (std::size_t i = 0; i < options_.members.size(); ++i)
    if (options_.members[i] == node) return *nodes_[i];
  assert(false && "not a ring member");
  return *nodes_.front();
}

bool RaftSimulator::IsUp(std::uint32_t node) const {
  return At(node).core != nullptr;
}

const RaftNode *RaftSimulator::Core(std::uint32_t node) const {
  return At(node).core.get();
}

const InMemoryRaftLog &RaftSimulator::Log(std::uint32_t node) const {
  return At(node).log;
}

void RaftSimulator::Schedule(SimulationEvent event) {
  event.sequence = sequence_++;
  queue_.push(std::move(event));
}

bool RaftSimulator::RunFor(std::uint64_t ticks) {
  const std::uint64_t end = now_ + ticks;
  while (!violation_ && !queue_.empty() && queue_.top().time <= end) {
    const SimulationEvent event = queue_.top();
    queue_.pop();
    now_ = event.time;
    ++counters_.events;
    Process(event);
    CheckInvariants();
  }
  if (!violation_) now_ = end;
  return !violation_;
}

void RaftSimulator::Process(const SimulationEvent &event) {
  SimulatedNode &node = At(event.node);
  switch (event.kind) {
    case SimulationEventKind::Tick:
      if (node.core && event.incarnation == node.incarnation)
        OnTick(event.node);
      return;
    case SimulationEventKind::DiskDone:
      if (node.core && event.incarnation == node.incarnation)
        OnDiskDone(event.node);
      return;
    case SimulationEventKind::Restart:
      Restart(event.node);
      return;
    case SimulationEventKind::Resume:
      if (node.core && event.incarnation == node.incarnation) {
        node.executorAway = false;
        Record(SimulationEventKind::Resume, event.node);
        Pump(event.node);
      }
      return;
    case SimulationEventKind::Deliver:
      if (!node.core || !Linked(event.message.from, event.node)) {
        ++counters_.dropped;
        Record(SimulationEventKind::Drop, event.node, &event.message);
        return;
      }
      ++counters_.delivered;
      Record(SimulationEventKind::Deliver, event.node, &event.message);
      node.core->Step(event.message);
      if (options_.traceWriter)
        options_.traceWriter->Step(event.node, event.message, *node.core,
                                   TakeSent(node));
      Pump(event.node);
      return;
    case SimulationEventKind::Crash:
    case SimulationEventKind::Drop:
    case SimulationEventKind::Duplicate:
    case SimulationEventKind::Wipe:
      assert(false && "not a queued event kind");
      return;
  }
}

void RaftSimulator::OnTick(std::uint32_t id) {
  SimulatedNode &node = At(id);
  if (options_.crashPpm != 0 && NodesDown() < options_.maxNodesDown &&
      random_.Chance(options_.crashPpm)) {
    Crash(id);
    Schedule({now_ + random_.Between(options_.restartDelayMinTicks,
                                     options_.restartDelayMaxTicks),
              0,
              SimulationEventKind::Restart,
              id,
              0,
              {}});
    return;
  }
  ++counters_.ticks;
  Record(SimulationEventKind::Tick, id);
  if (options_.traceWriter) {
    const std::string before = RaftTraceWriter::State(*node.core);
    node.core->Tick();
    std::vector<RaftMessage> sent = TakeSent(node);
    // A tick that changes nothing to be seen is left out of the trace.
    if (!sent.empty() || RaftTraceWriter::State(*node.core) != before)
      options_.traceWriter->Tick(id, *node.core, sent);
  } else {
    node.core->Tick();
  }
  Pump(id);
  const std::uint64_t jitter = options_.tickJitterTicks
                                   ? random_.Below(options_.tickJitterTicks + 1)
                                   : 0;
  Schedule({now_ + 1 + jitter,
            0,
            SimulationEventKind::Tick,
            id,
            node.incarnation,
            {}});
}

void RaftSimulator::OnDiskDone(std::uint32_t id) {
  SimulatedNode &node = At(id);
  assert(!node.writes.empty());
  SimulatedWrite write = std::move(node.writes.front());
  node.writes.pop_front();
  node.log.MarkDurableThrough(write.logSequence);
  if (write.hardState) node.log.SetHardState(*write.hardState);
  Record(SimulationEventKind::DiskDone, id);
  // Advance before the held-back messages leave: the trace releases them
  // with it.
  if (write.advanceOnDone) {
    node.core->Advance();
    if (options_.traceWriter)
      options_.traceWriter->Advance(id, *node.core, TakeSent(node));
  }
  SendAll(std::move(write.messages));
  if (write.advanceOnDone) {
    --node.awaiting;
    Advanced(id);
  }
}

void RaftSimulator::Advanced(std::uint32_t id) {
  SimulatedNode &node = At(id);
  if (options_.executorLagMaxTicks == 0) return Pump(id);
  node.executorAway = true;
  Schedule({now_ + random_.Between(0, options_.executorLagMaxTicks),
            0,
            SimulationEventKind::Resume,
            id,
            node.incarnation,
            {}});
}

void RaftSimulator::Pump(std::uint32_t id) {
  SimulatedNode &node = At(id);
  while (!violation_ && node.core && !node.executorAway &&
         node.core->HasReady()) {
    RaftReady ready = node.core->TakeReady();
    if (options_.traceWriter)
      options_.traceWriter->Take(id, ready, *node.core, TakeSent(node));
    Execute(id, std::move(ready));
  }
}

void RaftSimulator::Execute(std::uint32_t id, RaftReady ready) {
  SimulatedNode &node = At(id);
  if (ready.resetTo) {
    node.log.ResetTo(ready.resetTo->index, ready.resetTo->term);
    ++counters_.resets;
  }
  if (ready.truncateFrom) {
    const std::uint64_t from = *ready.truncateFrom;
    if (from <= node.log.LastIndex()) ++counters_.truncations;
    if (!node.log.TruncateFrom(from)) {
      Fail("node " + std::to_string(id) + " truncated from " +
           std::to_string(from) + ", below its first index");
      return;
    }
  }
  if (!ready.entriesToAppend.empty() &&
      !node.log.Append(ready.entriesToAppend)) {
    Fail("node " + std::to_string(id) +
         " appended entries that do not continue its log");
    return;
  }
  SendAll(std::move(ready.messages));
  const bool persist = ready.hardState || ready.truncateFrom || ready.resetTo ||
                       !ready.entriesToAppend.empty();
  // Advance goes in the order Readies were taken: behind a write still
  // in flight, even a Ready with nothing to write waits for it.
  const bool advanceNow = !ready.mustSync && node.awaiting == 0;
  if (persist || !advanceNow) {
    SimulatedWrite write;
    write.logSequence = node.log.WriteSequence();
    write.hardState = ready.hardState;
    if (!advanceNow) {
      write.messages = std::move(ready.messagesAfterSync);
      write.advanceOnDone = true;
      ++node.awaiting;
    }
    node.writes.push_back(std::move(write));
    if (persist) ++counters_.diskWrites;
    const std::uint64_t done =
        std::max(persist ? now_ + random_.Between(options_.diskDelayMinTicks,
                                                  options_.diskDelayMaxTicks)
                         : now_,
                 node.lastDiskDone);
    node.lastDiskDone = done;
    Schedule(
        {done, 0, SimulationEventKind::DiskDone, id, node.incarnation, {}});
  }
  if (advanceNow) {
    node.core->Advance();
    if (options_.traceWriter)
      options_.traceWriter->Advance(id, *node.core, TakeSent(node));
    SendAll(std::move(ready.messagesAfterSync));
    Advanced(id);
  }
}

void RaftSimulator::Observe(SimulatedNode &node) {
  node.sent.clear();
  if (!options_.traceWriter || !node.core) return;
  node.core->ObserveSends(
      [&node](const RaftMessage &message) { node.sent.push_back(message); });
}

std::vector<RaftMessage> RaftSimulator::TakeSent(SimulatedNode &node) {
  std::vector<RaftMessage> sent;
  sent.swap(node.sent);
  return sent;
}

void RaftSimulator::SendAll(std::vector<RaftMessage> messages) {
  for (const RaftMessage &message : messages) Send(message);
}

void RaftSimulator::Send(const RaftMessage &message) {
  ++counters_.sent;
  if (message.type == RaftMessageType::VoteResponse && message.granted) {
    if (auto broken = safety_.ObserveVoteGrant(message.from, message.storageId,
                                               message.term, message.to))
      Fail(std::move(*broken));
  }
  if (!Linked(message.from, message.to) || random_.Chance(options_.dropPpm)) {
    ++counters_.dropped;
    Record(SimulationEventKind::Drop, message.to, &message);
    return;
  }
  Schedule(
      {now_ + random_.Between(options_.delayMinTicks, options_.delayMaxTicks),
       0, SimulationEventKind::Deliver, message.to, 0, message});
  if (random_.Chance(options_.duplicatePpm)) {
    ++counters_.duplicated;
    Record(SimulationEventKind::Duplicate, message.to, &message);
    Schedule(
        {now_ + random_.Between(options_.delayMinTicks, options_.delayMaxTicks),
         0, SimulationEventKind::Deliver, message.to, 0, message});
  }
}

bool RaftSimulator::Propose(std::uint32_t id,
                            std::vector<std::uint8_t> payload) {
  SimulatedNode &node = At(id);
  if (!node.core) return false;
  const bool accepted = node.core->Propose(std::move(payload));
  if (options_.traceWriter)
    options_.traceWriter->Propose(id, accepted, *node.core, TakeSent(node));
  Pump(id);
  CheckInvariants();
  return accepted;
}

void RaftSimulator::TransferLeadership(std::uint32_t id,
                                       std::uint32_t successor) {
  SimulatedNode &node = At(id);
  if (!node.core) return;
  node.core->TransferLeadership(successor);
  if (options_.traceWriter)
    options_.traceWriter->Call("transfer", id, *node.core, TakeSent(node),
                               ",\"to\":" + std::to_string(successor));
  Pump(id);
  CheckInvariants();
}

void RaftSimulator::Crash(std::uint32_t id) {
  SimulatedNode &node = At(id);
  if (!node.core) return;
  node.core.reset();
  ++node.incarnation;
  node.awaiting = 0;
  node.executorAway = false;
  node.writes.clear();
  node.log.TrimToDurable();
  ++counters_.crashes;
  Record(SimulationEventKind::Crash, id);
  if (options_.traceWriter) options_.traceWriter->Crash(id);
}

void RaftSimulator::Restart(std::uint32_t id) {
  SimulatedNode &node = At(id);
  if (node.core) return;
  node.core =
      std::make_unique<RaftNode>(node.config, node.log, node.log.HardState());
  Observe(node);
  ++counters_.restarts;
  Record(SimulationEventKind::Restart, id);
  if (options_.traceWriter) options_.traceWriter->Restart(id, *node.core);
  Schedule({now_ + 1, 0, SimulationEventKind::Tick, id, node.incarnation, {}});
  Pump(id);
}

void RaftSimulator::Wipe(std::uint32_t id) {
  Crash(id);
  SimulatedNode &node = At(id);
  node.log = InMemoryRaftLog();
  node.config.storageId = nextStorage_++;
  Record(SimulationEventKind::Wipe, id);
}

void RaftSimulator::ContinueAlone(std::uint32_t id) {
  SimulatedNode &node = At(id);
  assert(!node.core);
  const RaftSoleVoterStart start = SoleVoterStart(
      node.log.HardState(), node.log.LastIndex(), id, node.config.storageId);
  node.log.Append({start.entry});
  node.log.MarkDurableThrough(node.log.WriteSequence());
  node.log.SetHardState(start.hardState);
  // The others' earlier commits no longer bind the observer to this cluster.
  safety_.ForgetCommitsAbove(start.entry.index - 1);
}

RaftMembershipOutcome RaftSimulator::ChangeMembership(
    std::uint32_t id, const RaftMembershipChange &change) {
  SimulatedNode &node = At(id);
  if (!node.core) return RaftMembershipOutcome::NotLeader;
  const RaftMembershipOutcome outcome = node.core->ChangeMembership(change);
  if (options_.traceWriter)
    options_.traceWriter->Call(
        "member", id, *node.core, TakeSent(node),
        ",\"kind\":" + std::to_string(static_cast<int>(change.kind)) +
            ",\"voter\":[" + std::to_string(change.voter.id) + "," +
            std::to_string(change.voter.storageId) +
            "],\"outcome\":" + std::to_string(static_cast<int>(outcome)));
  Pump(id);
  CheckInvariants();
  return outcome;
}

void RaftSimulator::SetMembers(std::uint32_t id,
                               std::vector<std::uint32_t> members) {
  SimulatedNode &node = At(id);
  node.config.members = members;
  if (!node.core) return;
  node.core->SetMembers(std::move(members));
  Pump(id);
  CheckInvariants();
}

bool RaftSimulator::Evict(std::uint32_t id, std::uint64_t index) {
  SimulatedNode &node = At(id);
  if (!node.core || index > node.log.HardState().commitIndex ||
      index > node.log.DurableLastIndex())
    return false;
  return node.log.EvictPrefixTo(index);
}

std::uint64_t RaftSimulator::StorageOf(std::uint32_t id) const {
  return At(id).config.storageId;
}

void RaftSimulator::Partition(
    const std::vector<std::vector<std::uint32_t>> &groups) {
  group_.clear();
  std::uint32_t next = 0;
  for (const auto &members : groups) {
    for (const std::uint32_t id : members) group_[id] = next;
    ++next;
  }
  for (const std::uint32_t id : options_.members)
    if (!group_.contains(id)) group_[id] = next++;
}

void RaftSimulator::Cut(std::uint32_t a, std::uint32_t b) {
  cuts_.insert(std::minmax(a, b));
}

void RaftSimulator::Mend(std::uint32_t a, std::uint32_t b) {
  cuts_.erase(std::minmax(a, b));
}

void RaftSimulator::Heal() {
  group_.clear();
  cuts_.clear();
}

void RaftSimulator::StopFaults() {
  options_.dropPpm = 0;
  options_.duplicatePpm = 0;
  options_.crashPpm = 0;
  Heal();
}

bool RaftSimulator::Linked(std::uint32_t a, std::uint32_t b) const {
  if (a == b) return true;
  if (cuts_.contains(std::minmax(a, b))) return false;
  if (group_.empty()) return true;
  const auto left = group_.find(a);
  const auto right = group_.find(b);
  return left != group_.end() && right != group_.end() &&
         left->second == right->second;
}

std::uint32_t RaftSimulator::NodesDown() const {
  std::uint32_t down = 0;
  for (const auto &node : nodes_)
    if (!node->core) ++down;
  return down;
}

void RaftSimulator::AddInvariant(std::string name, Invariant check) {
  invariants_.emplace_back(std::move(name), std::move(check));
}

void RaftSimulator::Record(SimulationEventKind kind, std::uint32_t node,
                           const RaftMessage *message) {
  SimulationTraceEvent event;
  event.time = now_;
  event.kind = kind;
  event.node = node;
  if (message) {
    event.from = message->from;
    event.type = message->type;
    event.term = message->term;
  }
  const std::uint64_t fields[] = {
      event.time, static_cast<std::uint64_t>(event.kind), event.node,
      event.from, static_cast<std::uint64_t>(event.type), event.term};
  for (const std::uint64_t field : fields) {
    for (int shift = 0; shift < 64; shift += 8) {
      digest_ ^= (field >> shift) & 0xFF;
      digest_ *= 1099511628211ull;
    }
  }
  if (options_.trace) trace_.push_back(event);
  if (options_.traceWriter && message) {
    if (kind == SimulationEventKind::Drop) options_.traceWriter->Drop(*message);
    if (kind == SimulationEventKind::Duplicate)
      options_.traceWriter->Duplicate(*message);
  }
}

void RaftSimulator::CheckInvariants() {
  if (violation_) return;
  for (std::size_t i = 0; i < nodes_.size(); ++i) {
    const SimulatedNode &node = *nodes_[i];
    if (!node.core) continue;
    const RaftNode &core = *node.core;
    if (auto broken = safety_.Observe(
            options_.members[i], node.incarnation, core.Role(), core.Term(),
            core.CommitIndex(),
            [&core](std::uint64_t index) { return core.TermAt(index); },
            core.FirstIndex())) {
      Fail(std::move(*broken));
      return;
    }
  }
  // What a leader commits is on the disks of a quorum of its voters. A
  // voter whose storage was wiped since may have held it: not counted
  // against.
  for (std::size_t i = 0; i < nodes_.size(); ++i) {
    SimulatedNode &node = *nodes_[i];
    if (!node.core) continue;
    const RaftNode &core = *node.core;
    const std::uint64_t commit = core.CommitIndex();
    if (core.Role() == RaftRole::Leader) {
      const std::vector<RaftVoter> &voters = core.Voters();
      for (std::uint64_t k =
               std::max(node.checkedCommit + 1, core.FirstIndex());
           k <= commit; ++k) {
        const std::optional<std::uint64_t> term = core.TermAt(k);
        std::size_t onDisk = 0;
        for (const RaftVoter &voter : voters)
          for (std::size_t j = 0; j < nodes_.size(); ++j) {
            const SimulatedNode &other = *nodes_[j];
            if (options_.members[j] != voter.id) continue;
            if (voter.storageId != other.config.storageId ||
                k < other.log.FirstIndex() ||
                other.log.DurableTermAt(k) == term)
              ++onDisk;
          }
        if (onDisk <= voters.size() / 2) {
          Fail("leader " + std::to_string(options_.members[i]) + " commits " +
               std::to_string(k) + " that " + std::to_string(onDisk) + " of " +
               std::to_string(voters.size()) + " voters hold on disk");
          return;
        }
      }
    }
    node.checkedCommit = std::max(node.checkedCommit, commit);
  }
  for (const auto &[name, check] : invariants_) {
    if (auto broken = check(*this)) {
      Fail(name + ": " + *broken);
      return;
    }
  }
}

void RaftSimulator::Fail(std::string what) {
  if (!violation_) violation_ = std::move(what);
}

std::string RaftSimulator::Report(std::size_t traceTail) const {
  std::ostringstream out;
  out << "seed=" << options_.seed << " step=" << counters_.events
      << " time=" << now_ << "\n";
  out << (violation_ ? "violation: " + *violation_ : "no violation") << "\n";
  const std::size_t first =
      trace_.size() > traceTail ? trace_.size() - traceTail : 0;
  for (std::size_t i = first; i < trace_.size(); ++i)
    out << "  " << trace_[i].ToString() << "\n";
  return out.str();
}

}  // namespace stepraft
