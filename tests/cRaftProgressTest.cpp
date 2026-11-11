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

#include "stepraft/cRaftProgress.hpp"

#include <gtest/gtest.h>

#include <deque>
#include <functional>
#include <memory>
#include <vector>

#include "stepraft/cInMemoryRaftLog.hpp"
#include "stepraft/cRaftNode.hpp"

namespace stepraft {
namespace {

TEST(RaftProgressTest, AProbeSendsOneMessageAndWaits) {
  RaftProgress progress(11, 4, 0);
  EXPECT_EQ(progress.State(), RaftReplicationState::Probe);
  progress.SentEntries(0, 0);
  EXPECT_FALSE(progress.Paused());
  progress.SentEntries(3, 300);
  EXPECT_TRUE(progress.Paused());
  EXPECT_EQ(progress.Next(), 11u);
  // Only a refusal of the probe that is out counts.
  EXPECT_FALSE(progress.MaybeDecrTo(7, 5));
  EXPECT_TRUE(progress.MaybeDecrTo(10, 6));
  EXPECT_EQ(progress.Next(), 6u);
  EXPECT_FALSE(progress.Paused());
}

TEST(RaftProgressTest, ReplicationStopsAtTheWindow) {
  RaftProgress progress(1, 3, 1000);
  ASSERT_TRUE(progress.MaybeUpdate(5));
  progress.BecomeReplicate();
  EXPECT_EQ(progress.Next(), 6u);
  progress.SentEntries(2, 100);
  progress.SentEntries(2, 100);
  EXPECT_FALSE(progress.Paused());
  progress.SentEntries(2, 100);
  EXPECT_TRUE(progress.Paused());  // three messages
  EXPECT_EQ(progress.Next(), 12u);
  ASSERT_TRUE(progress.MaybeUpdate(9));
  progress.FreeTo(9);
  EXPECT_EQ(progress.InflightMessages(), 1u);
  EXPECT_FALSE(progress.Paused());
  progress.SentEntries(1, 950);
  EXPECT_TRUE(progress.Paused());  // bytes
  // A refusal at or below what is known held is stale; above it, back to
  // just past the match.
  EXPECT_FALSE(progress.MaybeDecrTo(9, 1));
  EXPECT_TRUE(progress.MaybeDecrTo(12, 1));
  EXPECT_EQ(progress.Next(), 10u);
}

TEST(RaftProgressTest, ANewStorageStartsTheFollowerOver) {
  RaftProgress progress(1, 3, 0);
  EXPECT_FALSE(progress.ObserveStorage(7, 40));  // first seen
  ASSERT_TRUE(progress.MaybeUpdate(30));
  progress.BecomeReplicate();
  EXPECT_FALSE(progress.ObserveStorage(7, 40));
  EXPECT_FALSE(progress.ObserveStorage(0, 40));  // not reported
  EXPECT_TRUE(progress.ObserveStorage(8, 40));
  EXPECT_EQ(progress.Match(), 0u);
  EXPECT_EQ(progress.Next(), 40u);
  EXPECT_EQ(progress.State(), RaftReplicationState::Probe);
}

// Three cores over in-memory logs, every Ready executed and synced at
// once; `drop` decides which messages are lost.
class Ring {
 public:
  explicit Ring(RaftConfig base) : base_(std::move(base)) {
    for (std::uint32_t id = 1; id <= 3; ++id) Restart(id, 10 + id, false);
  }

  void Restart(std::uint32_t id, std::uint64_t storageId, bool wiped) {
    Node &node = nodes_[id - 1];
    if (wiped) node.log = std::make_unique<InMemoryRaftLog>();
    if (!node.log) node.log = std::make_unique<InMemoryRaftLog>();
    RaftConfig config = base_;
    config.selfId = id;
    config.members = {1, 2, 3};
    config.voters = {{1, 11}, {2, 12}, {3, 13}};
    config.randomSeed = 1000 + id;
    config.storageId = storageId;
    node.core = std::make_unique<RaftNode>(
        config, *node.log, wiped ? RaftHardState{} : node.log->HardState());
  }

  RaftNode &Core(std::uint32_t id) { return *nodes_[id - 1].core; }
  InMemoryRaftLog &Log(std::uint32_t id) { return *nodes_[id - 1].log; }

  void Run(std::size_t ticks) {
    for (std::size_t i = 0; i < ticks; ++i) {
      for (auto &node : nodes_) node.core->Tick();
      Deliver();
    }
  }

  std::uint32_t Leader() {
    for (std::uint32_t id = 1; id <= 3; ++id)
      if (Core(id).Role() == RaftRole::Leader) return id;
    return 0;
  }

  void Deliver() {
    Pump();
    while (!queue_.empty()) {
      RaftMessage message = std::move(queue_.front());
      queue_.pop_front();
      if (drop && drop(message)) continue;
      Core(message.to).Step(message);
      Pump();
    }
  }

  std::function<bool(const RaftMessage &)> drop;

 private:
  struct Node {
    std::unique_ptr<InMemoryRaftLog> log;
    std::unique_ptr<RaftNode> core;
  };

  void Pump() {
    for (auto &node : nodes_) {
      while (node.core->HasReady()) {
        RaftReady ready = node.core->TakeReady();
        if (ready.truncateFrom) node.log->TruncateFrom(*ready.truncateFrom);
        node.log->Append(ready.entriesToAppend);
        node.log->MarkDurableThrough(node.log->WriteSequence());
        if (ready.hardState) node.log->SetHardState(*ready.hardState);
        for (RaftMessage &message : ready.messages)
          queue_.push_back(std::move(message));
        for (RaftMessage &message : ready.messagesAfterSync)
          queue_.push_back(std::move(message));
        node.core->Advance();
      }
    }
  }

  RaftConfig base_;
  Node nodes_[3];
  std::deque<RaftMessage> queue_;
};

RaftConfig SmallWindow() {
  RaftConfig config;
  config.maxAppendBytes = 1;  // one entry per message
  config.maxInflightMessages = 4;
  return config;
}

std::vector<std::uint8_t> Payload(std::uint64_t value) {
  return {static_cast<std::uint8_t>(value),
          static_cast<std::uint8_t>(value >> 8)};
}

bool SameLogs(Ring &ring) {
  const auto reference = ring.Log(1).Read(1, ring.Log(1).LastIndex(), ~0ULL);
  for (std::uint32_t id = 2; id <= 3; ++id)
    if (ring.Log(id).Read(1, ring.Log(id).LastIndex(), ~0ULL) != reference)
      return false;
  return true;
}

TEST(RaftNodeWindowTest, NoMoreThanTheWindowIsInFlightToASilentFollower) {
  Ring ring(SmallWindow());
  ring.Run(100);
  const std::uint32_t leader = ring.Leader();
  ASSERT_NE(leader, 0u);
  const std::uint32_t silent = leader == 3 ? 2 : 3;
  for (std::uint64_t value = 1; value <= 3; ++value)
    ASSERT_TRUE(ring.Core(leader).Propose(Payload(value)));
  ring.Deliver();
  ASSERT_TRUE(SameLogs(ring));

  // The follower still receives, but its answers are lost.
  std::size_t carrying = 0;
  ring.drop = [&](const RaftMessage &message) {
    if (message.to == silent && !message.entries.empty()) ++carrying;
    return message.from == silent;
  };
  for (std::uint64_t value = 10; value < 60; ++value) {
    ASSERT_TRUE(ring.Core(leader).Propose(Payload(value)));
    ring.Deliver();
  }
  ring.Run(5);
  EXPECT_LE(carrying, 4u);
  EXPECT_EQ(ring.Core(leader).CommitIndex(), ring.Core(leader).LastIndex());

  ring.drop = nullptr;
  ring.Run(50);
  EXPECT_TRUE(SameLogs(ring));
  EXPECT_EQ(ring.Core(silent).CommitIndex(), ring.Core(leader).LastIndex());
}

TEST(RaftNodeWindowTest, LostMessagesAreSentAgainAfterAHeartbeat) {
  Ring ring(SmallWindow());
  ring.Run(100);
  const std::uint32_t leader = ring.Leader();
  ASSERT_NE(leader, 0u);
  const std::uint32_t losing = leader == 3 ? 2 : 3;
  ASSERT_TRUE(ring.Core(leader).Propose(Payload(1)));
  ring.Deliver();
  // Entries to the follower are lost; heartbeats get through.
  ring.drop = [&](const RaftMessage &message) {
    return message.to == losing && !message.entries.empty();
  };
  for (std::uint64_t value = 2; value < 30; ++value) {
    ASSERT_TRUE(ring.Core(leader).Propose(Payload(value)));
    ring.Deliver();
  }
  ring.drop = nullptr;
  ring.Run(60);
  EXPECT_TRUE(SameLogs(ring));
  EXPECT_EQ(ring.Leader(), leader);
}

TEST(RaftNodeWindowTest, AFollowerOnANewStorageCatchesUpUnderTheSameLeader) {
  for (const bool newStorage : {false, true}) {
    Ring ring(SmallWindow());
    ring.Run(100);
    const std::uint32_t leader = ring.Leader();
    ASSERT_NE(leader, 0u);
    const std::uint32_t wiped = leader == 3 ? 2 : 3;
    for (std::uint64_t value = 1; value <= 10; ++value)
      ASSERT_TRUE(ring.Core(leader).Propose(Payload(value)));
    ring.Deliver();
    ring.Run(5);
    ASSERT_TRUE(SameLogs(ring));
    const std::uint64_t term = ring.Core(leader).Term();

    ring.Restart(wiped, newStorage ? 99 : 10 + wiped, true);
    ring.Run(60);
    EXPECT_EQ(ring.Leader(), leader);
    EXPECT_EQ(ring.Core(leader).Term(), term);
    if (newStorage) {
      EXPECT_TRUE(SameLogs(ring));
    } else {
      // Same storage named, log gone: the leader still believes what the
      // follower acknowledged before and takes its refusals as stale.
      EXPECT_EQ(ring.Log(wiped).LastIndex(), 0u);
    }
  }
}

}  // namespace
}  // namespace stepraft
