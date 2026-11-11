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

#include <gtest/gtest.h>

#include "hRaftScenario.hpp"

namespace stepraft {
namespace {

using namespace scenario;

TEST(RaftNodeReplicationTest, CalmRingReplicatesAndCommitsInOrder) {
  for (const std::uint32_t nodes : {3u, 5u}) {
    for (std::uint64_t seed = 1; seed <= 5; ++seed) {
      RaftSimulator sim(Ring(seed, nodes));
      const auto leader = RunUntilLeader(sim, 100);
      ASSERT_TRUE(leader) << sim.Report();
      for (std::uint64_t value = 1; value <= 20; ++value) {
        ASSERT_TRUE(sim.Propose(*leader, Payload(value)));
        ASSERT_TRUE(sim.RunFor(1)) << sim.Report();
      }
      ASSERT_TRUE(sim.RunFor(30)) << sim.Report();
      ASSERT_EQ(sim.Core(*leader)->LastIndex(), 21u);  // no-op + 20
      EXPECT_TRUE(LogsIdentical(sim));
      for (const std::uint32_t id : sim.Members()) {
        EXPECT_EQ(sim.Core(id)->CommitIndex(), 21u) << "node " << id;
        const auto entries = sim.Log(id).Read(1, 21, 1 << 20);
        ASSERT_EQ(entries.size(), 21u);
        EXPECT_EQ(entries[0].kind, RaftEntryKind::NoOp);
        for (std::uint64_t value = 1; value <= 20; ++value)
          EXPECT_EQ(entries[value].payload, Payload(value));
      }
    }
  }
}

// One payload entry per message: catching up takes many rounds, and each
// round may only commit what the follower has verified so far.
TEST(RaftNodeReplicationTest, LaggingFollowerCatchesUp) {
  SimulationOptions options = Ring(2);
  options.raft.maxAppendBytes = 8;
  RaftSimulator sim(options);
  const auto leader = RunUntilLeader(sim, 100);
  ASSERT_TRUE(leader);
  const std::uint32_t lagging = OtherThan(sim, {*leader});
  sim.Partition({{lagging}, AllBut(sim, lagging)});
  for (std::uint64_t value = 1; value <= 50; ++value) {
    ASSERT_TRUE(sim.Propose(*leader, Payload(value)));
    ASSERT_TRUE(sim.RunFor(1));
  }
  ASSERT_TRUE(sim.RunFor(20));
  EXPECT_EQ(sim.Core(*leader)->CommitIndex(), 51u);
  EXPECT_LT(sim.Core(lagging)->LastIndex(), 51u);

  sim.Heal();
  ASSERT_TRUE(sim.RunFor(60)) << sim.Report();
  EXPECT_TRUE(LogsIdentical(sim));
  EXPECT_EQ(sim.Core(lagging)->CommitIndex(), 51u);
  EXPECT_EQ(AgreedLeader(sim), leader);
}

// The old leader is cut off with entries no one else has; the majority
// moves on in a new term. On return, the old tail is cut and replaced.
struct ConflictingTail {
  std::uint32_t oldLeader = 0;
  std::uint64_t oldTerm = 0;
  // First index of the old leader's uncommitted tail.
  std::uint64_t tailStart = 0;
};

ConflictingTail SplitWithConflictingTail(RaftSimulator &sim) {
  ConflictingTail tail;
  const auto leader = RunUntilLeader(sim, 1000);
  EXPECT_TRUE(leader) << sim.Report();
  if (!leader) return tail;
  EXPECT_TRUE(sim.RunFor(30));
  tail.oldLeader = *leader;
  tail.oldTerm = sim.Core(*leader)->Term();
  tail.tailStart = sim.Core(*leader)->CommitIndex() + 1;
  const auto rest = AllBut(sim, *leader);
  sim.Partition({{*leader}, rest});
  for (std::uint64_t value = 100; value < 105; ++value)
    EXPECT_TRUE(sim.Propose(*leader, Payload(value)));
  for (int tick = 0; tick < 1000 && sim.Core(rest.front())->LeaderId() == 0;
       ++tick)
    EXPECT_TRUE(sim.RunFor(1));
  EXPECT_TRUE(sim.RunFor(150)) << sim.Report();
  const std::uint32_t newLeader = sim.Core(rest.front())->LeaderId();
  EXPECT_NE(newLeader, 0u) << sim.Report();
  EXPECT_NE(newLeader, *leader);
  if (newLeader == 0) return tail;
  for (std::uint64_t value = 200; value < 203; ++value)
    EXPECT_TRUE(sim.Propose(newLeader, Payload(value)));
  EXPECT_TRUE(sim.RunFor(60));
  EXPECT_EQ(sim.Core(*leader)->TermAt(tail.tailStart), tail.oldTerm);
  return tail;
}

TEST(RaftNodeReplicationTest, ConflictingTailIsReplaced) {
  RaftSimulator sim(Ring(3));
  const ConflictingTail tail = SplitWithConflictingTail(sim);
  ASSERT_NE(tail.oldLeader, 0u);
  sim.Heal();
  ASSERT_TRUE(sim.RunFor(100)) << sim.Report();
  EXPECT_TRUE(LogsIdentical(sim)) << sim.Report();
  EXPECT_NE(sim.Core(tail.oldLeader)->TermAt(tail.tailStart), tail.oldTerm);
  EXPECT_GT(sim.Counters().truncations, 0u);
}

// The cut is prescribed but has not reached the disk when the node
// crashes: the old tail comes back on restart, and the leader cuts it
// again. Nothing was acknowledged on the strength of the lost cut.
TEST(RaftNodeReplicationTest, TruncationLostInACrashIsRedone) {
  SimulationOptions options = Ring(4);
  options.diskDelayMinTicks = 8;
  options.diskDelayMaxTicks = 8;
  // An election needs two syncs in a row (the candidate's term and vote,
  // then the voter's); the timeout must leave room for both.
  options.raft.electionTimeoutMinTicks = 50;
  options.raft.electionTimeoutMaxTicks = 100;
  RaftSimulator sim(options);
  const ConflictingTail tail = SplitWithConflictingTail(sim);
  ASSERT_NE(tail.oldLeader, 0u);
  const RaftNode *old = sim.Core(tail.oldLeader);
  sim.Heal();
  for (int tick = 0; tick < 1000 && old->TermAt(tail.tailStart) == tail.oldTerm;
       ++tick)
    ASSERT_TRUE(sim.RunFor(1)) << sim.Report();
  ASSERT_NE(old->TermAt(tail.tailStart), tail.oldTerm) << sim.Report();

  sim.Crash(tail.oldLeader);
  EXPECT_EQ(sim.Log(tail.oldLeader).TermAt(tail.tailStart), tail.oldTerm);
  sim.Restart(tail.oldLeader);
  EXPECT_EQ(sim.Core(tail.oldLeader)->TermAt(tail.tailStart), tail.oldTerm);

  ASSERT_TRUE(sim.RunFor(500)) << sim.Report();
  EXPECT_TRUE(LogsIdentical(sim)) << sim.Report();
  EXPECT_NE(sim.Core(tail.oldLeader)->TermAt(tail.tailStart), tail.oldTerm);
  EXPECT_GE(sim.Counters().truncations, 2u);
}

// Maintenance: the leader hands over, then leaves; the rest of the cluster
// keeps committing and nothing committed is lost.
TEST(RaftNodeReplicationTest, TransferLetsTheLeaderLeaveForMaintenance) {
  RaftSimulator sim(Ring(5));
  const auto leader = RunUntilLeader(sim, 100);
  ASSERT_TRUE(leader);
  for (std::uint64_t value = 1; value <= 10; ++value)
    ASSERT_TRUE(sim.Propose(*leader, Payload(value)));
  ASSERT_TRUE(sim.RunFor(20));
  const std::uint64_t term = sim.Core(*leader)->Term();
  const std::uint64_t committed = sim.Core(*leader)->CommitIndex();
  ASSERT_EQ(committed, 11u);

  const std::uint32_t successor = OtherThan(sim, {*leader});
  sim.TransferLeadership(*leader, successor);
  ASSERT_TRUE(sim.RunFor(30)) << sim.Report();
  EXPECT_EQ(AgreedLeader(sim), successor) << sim.Report();
  EXPECT_EQ(sim.Core(successor)->Term(), term + 1);
  EXPECT_GT(sim.Core(successor)->CommitIndex(), committed);

  sim.Crash(*leader);
  for (std::uint64_t value = 11; value <= 15; ++value)
    ASSERT_TRUE(sim.Propose(successor, Payload(value)));
  ASSERT_TRUE(sim.RunFor(20));
  EXPECT_EQ(sim.Core(successor)->CommitIndex(),
            sim.Core(successor)->LastIndex());
  EXPECT_TRUE(LogsIdentical(sim));
}

TEST(RaftNodeReplicationTest, TransferFirstBringsTheSuccessorUpToDate) {
  RaftSimulator sim(Ring(6));
  const auto leader = RunUntilLeader(sim, 100);
  ASSERT_TRUE(leader);
  const std::uint32_t successor = OtherThan(sim, {*leader});
  sim.Cut(*leader, successor);
  for (std::uint64_t value = 1; value <= 30; ++value)
    ASSERT_TRUE(sim.Propose(*leader, Payload(value)));
  ASSERT_TRUE(sim.RunFor(10));
  ASSERT_LT(sim.Core(successor)->LastIndex(), sim.Core(*leader)->LastIndex());

  sim.Mend(*leader, successor);
  sim.TransferLeadership(*leader, successor);
  EXPECT_FALSE(sim.Propose(*leader, Payload(999)));  // not while handing over
  ASSERT_TRUE(sim.RunFor(40)) << sim.Report();
  EXPECT_EQ(AgreedLeader(sim), successor) << sim.Report();
  EXPECT_TRUE(LogsIdentical(sim));
  EXPECT_EQ(sim.Core(successor)->CommitIndex(),
            sim.Core(successor)->LastIndex());
}

TEST(RaftNodeReplicationTest, TransferToADownNodeIsAbandoned) {
  RaftSimulator sim(Ring(7));
  const auto leader = RunUntilLeader(sim, 100);
  ASSERT_TRUE(leader);
  const std::uint32_t target = OtherThan(sim, {*leader});
  sim.Crash(target);
  sim.TransferLeadership(*leader, target);
  EXPECT_FALSE(sim.Propose(*leader, Payload(1)));
  ASSERT_TRUE(sim.RunFor(15));
  EXPECT_EQ(AgreedLeader(sim), leader);
  EXPECT_TRUE(sim.Propose(*leader, Payload(2)));
}

enum class Figure8 { ShapeMissed, Passed, Violated };

std::optional<std::uint32_t> LeaderAmong(const RaftSimulator &sim,
                                         std::vector<std::uint32_t> nodes) {
  for (const std::uint32_t id : nodes)
    if (const RaftNode *core = sim.Core(id);
        core && core->Role() == RaftRole::Leader)
      return id;
  return std::nullopt;
}

std::optional<std::uint32_t> WaitForLeaderAmong(
    RaftSimulator &sim, const std::vector<std::uint32_t> &nodes) {
  for (int tick = 0; tick < 1000; ++tick) {
    if (const auto leader = LeaderAmong(sim, nodes)) return leader;
    if (!sim.RunFor(1)) return std::nullopt;
  }
  return std::nullopt;
}

// Figure 8 of the Raft paper: an entry of an older term reaches a
// majority under a newer leader, the leader dies before an entry of its
// own term follows, and a node holding a different entry at that index
// is then elected. Counting replicas of the old entry as a commit would
// lose it. Which node plays which part depends on the run, so the script
// adapts and reports when the run did not take the needed shape.
Figure8 RunFigure8(std::uint64_t seed) {
  SimulationOptions options = Ring(seed, 5);
  options.raft.maxAppendBytes = 1;  // one payload entry per message
  options.raft.heartbeatTicks = 8;
  options.raft.electionTimeoutMinTicks = 30;
  options.raft.electionTimeoutMaxTicks = 60;
  RaftSimulator sim(options);
  const auto violated = [&sim] { return sim.Violation().has_value(); };

  const auto first = RunUntilLeader(sim, 1000);
  if (!first || !sim.RunFor(80)) return Figure8::ShapeMissed;
  const std::uint32_t a = *first;
  const std::uint64_t base = sim.Core(a)->LastIndex();
  if (sim.Core(a)->CommitIndex() != base) return Figure8::ShapeMissed;
  const std::uint32_t b = OtherThan(sim, {a});
  std::vector<std::uint32_t> rest = AllBut(sim, a);
  std::erase(rest, b);

  // A writes entry base+1 in its term, and only B gets it.
  sim.Partition({{a, b}, rest});
  if (!sim.Propose(a, Payload(1)) || !sim.RunFor(4)) return Figure8::Violated;
  if (sim.Core(b)->LastIndex() != base + 1) return Figure8::ShapeMissed;
  sim.Crash(a);

  // W is elected by the other three and writes its no-op at base+1, but
  // is cut off before anyone receives it, then dies.
  const auto w = WaitForLeaderAmong(sim, rest);
  if (violated()) return Figure8::Violated;
  if (!w) return Figure8::ShapeMissed;
  sim.Partition({{*w}, AllBut(sim, *w)});
  if (!sim.RunFor(3)) return Figure8::Violated;
  sim.Crash(*w);
  std::erase(rest, *w);
  const std::uint32_t c = rest[0];
  const std::uint32_t d = rest[1];

  // A or B leads a newer term and copies the old entry to C: on a
  // majority now, but C has none of the new leader's own entries yet.
  sim.Restart(a);
  sim.Partition({{a, b, c}, {d}});
  const auto leader = WaitForLeaderAmong(sim, {a, b});
  if (violated()) return Figure8::Violated;
  if (!leader) return Figure8::ShapeMissed;
  for (int tick = 0; tick < 300 && sim.Log(c).DurableLastIndex() <= base;
       ++tick)
    if (!sim.RunFor(1)) return Figure8::Violated;
  if (sim.Log(c).DurableLastIndex() != base + 1 ||
      sim.Core(c)->LastIndex() != base + 1)
    return Figure8::ShapeMissed;
  if (!sim.RunFor(1)) return Figure8::Violated;  // the leader hears C's ack
  sim.Crash(*leader);
  sim.Partition({{c, d}});
  if (sim.Core(c)->LastIndex() != base + 1) return Figure8::ShapeMissed;

  // W comes back; its no-op at base+1 is newer than C's entry, so C and D
  // elect it.
  sim.Restart(*w);
  sim.Partition({{*w, c, d}});
  const auto last = WaitForLeaderAmong(sim, {*w, c, d});
  if (violated()) return Figure8::Violated;
  if (!last) return Figure8::ShapeMissed;
  if (!sim.RunFor(100)) return Figure8::Violated;
  return Figure8::Passed;
}

TEST(RaftNodeReplicationTest, Figure8OldTermEntryIsNotCommittedByCounting) {
  int shaped = 0;
  for (std::uint64_t seed = 1; seed <= 40; ++seed) {
    const Figure8 result = RunFigure8(seed);
    EXPECT_NE(result, Figure8::Violated) << "seed " << seed;
    if (result != Figure8::ShapeMissed) ++shaped;
  }
  EXPECT_GE(shaped, 3);
}

}  // namespace
}  // namespace stepraft
