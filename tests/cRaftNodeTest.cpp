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

#include "stepraft/cRaftNode.hpp"

#include <gtest/gtest.h>

#include <vector>
#include "stepraft/cInMemoryRaftLog.hpp"

namespace stepraft {
namespace {

RaftConfig RingOfThree(std::uint32_t selfId = 1) {
  RaftConfig config;
  config.selfId = selfId;
  config.members = {1, 2, 3};
  config.storageId = 10 + selfId;
  config.voters = {{1, 11}, {2, 12}, {3, 13}};
  return config;
}

TEST(RaftConfigTest, ValidityAndQuorum) {
  const auto quorum = [](const RaftConfig &ring) {
    return RaftMembership::Quorum(ring.voters);
  };
  RaftConfig config = RingOfThree();
  EXPECT_TRUE(config.Valid());
  EXPECT_EQ(quorum(config), 2u);

  config.voters = {{1, 11}, {4, 44}};  // a voter that is no member
  EXPECT_FALSE(config.Valid());
  config.voters = {{1, 11}, {2, 0}};  // a voter on no storage
  EXPECT_FALSE(config.Valid());
  config.voters = {{1, 11}, {2, 22}};
  EXPECT_TRUE(config.Valid());
  config.storageId = 0;
  EXPECT_FALSE(config.Valid());
  config.storageId = 11;
  config.voters.clear();

  config.members = {1};  // a cluster others join later
  EXPECT_TRUE(config.Valid());
  config.members.clear();
  EXPECT_FALSE(config.Valid());

  config.members = {2, 3, 4};  // self not a member
  EXPECT_FALSE(config.Valid());

  config = RingOfThree();
  config.members = {1, 2, 3, 4, 5};
  config.voters = {{1, 11}, {2, 12}, {3, 13}, {4, 14}, {5, 15}};
  EXPECT_EQ(quorum(config), 3u);
}

TEST(RaftNodeTest, FreshNodeIsQuietFollower) {
  InMemoryRaftLog log;
  RaftNode node(RingOfThree(), log, RaftHardState{});
  EXPECT_EQ(node.Role(), RaftRole::Follower);
  EXPECT_EQ(node.Term(), 0u);
  EXPECT_FALSE(node.HasReady());
  node.Tick();
  EXPECT_FALSE(node.HasReady());
}

// A log keeps its entries until they are committed, so whatever it
// evicted was: a restart counts its base committed, as etcd's newLog does,
// even when the stored commit point lags.
TEST(RaftNodeTest, ARestartCountsTheLogsBaseCommitted) {
  InMemoryRaftLog log;
  std::vector<RaftLogEntry> entries;
  for (std::uint64_t index = 1; index <= 8; ++index) {
    RaftLogEntry entry;
    entry.index = index;
    entry.term = 1;
    entry.payload = {static_cast<std::uint8_t>(index)};
    entries.push_back(entry);
  }
  ASSERT_TRUE(log.Append(entries));
  log.MarkDurableThrough(log.WriteSequence());
  ASSERT_TRUE(log.EvictPrefixTo(5));
  RaftHardState state;
  state.term = 1;
  state.commitIndex = 2;
  RaftNode node(RingOfThree(), log, state);
  EXPECT_EQ(node.FirstIndex(), 6u);
  EXPECT_EQ(node.CommitIndex(), 5u);
}

TEST(RaftNodeTest, ProposeIsRefusedOnNonLeader) {
  InMemoryRaftLog log;
  RaftNode node(RingOfThree(), log, RaftHardState{});
  EXPECT_FALSE(node.Propose({0x01, 0x02}));
}

TEST(RaftNodeTest, HigherTermIsAdoptedAndPrescribedDurable) {
  InMemoryRaftLog log;
  RaftHardState recovered;
  recovered.term = 3;
  recovered.votedFor = 2;
  RaftNode node(RingOfThree(), log, recovered);

  RaftMessage message;
  message.type = RaftMessageType::AppendEntries;
  message.from = 2;
  message.to = 1;
  message.term = 5;
  node.Step(message);

  EXPECT_EQ(node.Term(), 5u);
  EXPECT_EQ(node.Role(), RaftRole::Follower);
  ASSERT_TRUE(node.HasReady());
  RaftReady ready = node.TakeReady();
  ASSERT_TRUE(ready.hardState.has_value());
  EXPECT_EQ(ready.hardState->term, 5u);
  // The vote of the old term does not leak into the new one.
  EXPECT_FALSE(ready.hardState->votedFor.has_value());
  // Term/vote changes must be durable before anything is sent.
  EXPECT_TRUE(ready.mustSync);
  node.Advance();
  EXPECT_FALSE(node.HasReady());
}

// With no leader to hold a lease, a pre-vote probe reaches the vote
// logic; answering it must still leave the receiver's term alone.
TEST(RaftNodeTest, PreVoteRequestLeavesTheReceiversTermAlone) {
  InMemoryRaftLog log;
  RaftNode node(RingOfThree(), log, RaftHardState{});
  RaftMessage probe;
  probe.type = RaftMessageType::PreVoteRequest;
  probe.from = 2;
  probe.to = 1;
  probe.term = 7;
  node.Step(probe);
  EXPECT_EQ(node.Term(), 0u);
  ASSERT_TRUE(node.HasReady());
  const RaftReady ready = node.TakeReady();
  EXPECT_FALSE(ready.hardState);
  EXPECT_TRUE(ready.messages.empty());
  ASSERT_EQ(ready.messagesAfterSync.size(), 1u);
  EXPECT_EQ(ready.messagesAfterSync[0].type, RaftMessageType::PreVoteResponse);
  EXPECT_TRUE(ready.messagesAfterSync[0].granted);
  EXPECT_EQ(ready.messagesAfterSync[0].term, 7u);
}

TEST(RaftNodeTest, StaleTermIsIgnored) {
  InMemoryRaftLog log;
  RaftHardState recovered;
  recovered.term = 4;
  RaftNode node(RingOfThree(), log, recovered);

  RaftMessage message;
  message.type = RaftMessageType::VoteRequest;
  message.from = 3;
  message.to = 1;
  message.term = 2;
  node.Step(message);

  EXPECT_EQ(node.Term(), 4u);
  EXPECT_FALSE(node.HasReady());
}

TEST(RaftNodeTest, EventsDuringExecutionOpenTheNextReady) {
  InMemoryRaftLog log;
  RaftNode node(RingOfThree(), log, RaftHardState{});

  RaftMessage first;
  first.type = RaftMessageType::AppendEntries;
  first.from = 2;
  first.to = 1;
  first.term = 1;
  node.Step(first);
  ASSERT_TRUE(node.HasReady());
  RaftReady taken = node.TakeReady();
  ASSERT_TRUE(taken.hardState.has_value());

  // While the executor works on `taken`, a new event arrives.
  RaftMessage second = first;
  second.term = 2;
  node.Step(second);

  node.Advance();
  ASSERT_TRUE(node.HasReady());
  RaftReady next = node.TakeReady();
  ASSERT_TRUE(next.hardState.has_value());
  EXPECT_EQ(next.hardState->term, 2u);
  node.Advance();
  EXPECT_FALSE(node.HasReady());
}

// A follower whose write is still in flight answers heartbeats at once;
// its acknowledgement of the entries waits for their sync.
TEST(RaftNodeTest, TakesTheNextReadyWhileOneIsWritten) {
  InMemoryRaftLog log;
  RaftHardState recovered;
  recovered.term = 1;
  RaftNode node(RingOfThree(), log, recovered);

  RaftMessage append;
  append.type = RaftMessageType::AppendEntries;
  append.from = 2;
  append.to = 1;
  append.term = 1;
  RaftLogEntry entry;
  entry.term = 1;
  entry.index = 1;
  entry.payload = {1, 2, 3};
  append.entries.push_back(entry);
  node.Step(append);
  ASSERT_TRUE(node.HasReady());
  const RaftReady writing = node.TakeReady();
  EXPECT_TRUE(writing.mustSync);
  ASSERT_EQ(writing.entriesToAppend.size(), 1u);
  EXPECT_TRUE(writing.messages.empty());
  ASSERT_EQ(writing.messagesAfterSync.size(), 1u);
  EXPECT_EQ(writing.messagesAfterSync[0].matchIndex, 1u);

  RaftMessage heartbeat;
  heartbeat.type = RaftMessageType::AppendEntries;
  heartbeat.heartbeat = true;
  heartbeat.from = 2;
  heartbeat.to = 1;
  heartbeat.term = 1;
  node.Step(heartbeat);
  ASSERT_TRUE(node.HasReady());
  const RaftReady answer = node.TakeReady();
  EXPECT_TRUE(answer.entriesToAppend.empty());
  ASSERT_EQ(answer.messages.size(), 1u);
  EXPECT_TRUE(answer.messages[0].heartbeat);
  EXPECT_TRUE(answer.messages[0].success);
  EXPECT_TRUE(answer.messagesAfterSync.empty());
  EXPECT_EQ(node.LastIndex(), 1u);

  ASSERT_TRUE(log.Append(writing.entriesToAppend));
  node.Advance();
  node.Advance();
  EXPECT_FALSE(node.HasReady());
  EXPECT_EQ(node.LastIndex(), 1u);
  EXPECT_EQ(node.TermAt(1), 1u);
}

}  // namespace
}  // namespace stepraft
