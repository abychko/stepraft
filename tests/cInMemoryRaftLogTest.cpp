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

#include "stepraft/cInMemoryRaftLog.hpp"

#include <gtest/gtest.h>

namespace stepraft {
namespace {

RaftLogEntry Entry(std::uint64_t term, std::uint64_t index,
                   std::size_t payloadBytes = 0) {
  RaftLogEntry entry;
  entry.term = term;
  entry.index = index;
  entry.kind = payloadBytes == 0 ? RaftEntryKind::NoOp : RaftEntryKind::Payload;
  entry.payload.assign(payloadBytes, 0xAB);
  return entry;
}

TEST(InMemoryRaftLogTest, EmptyLog) {
  InMemoryRaftLog log;
  EXPECT_EQ(log.FirstIndex(), 0u);
  EXPECT_EQ(log.LastIndex(), 0u);
  EXPECT_EQ(log.TermAt(0), std::optional<std::uint64_t>(0));
  EXPECT_EQ(log.TermAt(1), std::nullopt);
  EXPECT_TRUE(log.Read(1, 1, 1024).empty());
}

TEST(InMemoryRaftLogTest, AppendRequiresContiguousIndexes) {
  InMemoryRaftLog log;
  EXPECT_TRUE(log.Append({Entry(1, 1), Entry(1, 2)}));
  EXPECT_EQ(log.LastIndex(), 2u);
  EXPECT_FALSE(log.Append({Entry(1, 4)}));
  EXPECT_EQ(log.LastIndex(), 2u);
  EXPECT_TRUE(log.Append({Entry(2, 3)}));
  EXPECT_EQ(log.TermAt(3), std::optional<std::uint64_t>(2));
}

TEST(InMemoryRaftLogTest, ReadRangeHonoursByteLimit) {
  InMemoryRaftLog log;
  ASSERT_TRUE(
      log.Append({Entry(1, 1, 100), Entry(1, 2, 100), Entry(1, 3, 100)}));
  // The limit cuts after the entry that crosses it, but at least one
  // entry is returned.
  EXPECT_EQ(log.Read(1, 3, 150).size(), 1u);
  EXPECT_EQ(log.Read(1, 3, 200).size(), 2u);
  EXPECT_EQ(log.Read(1, 3, 10).size(), 1u);
  EXPECT_EQ(log.Read(1, 3, 1000).size(), 3u);
  EXPECT_TRUE(log.Read(2, 1, 1000).empty());
  EXPECT_TRUE(log.Read(1, 4, 1000).empty());
}

TEST(InMemoryRaftLogTest, ExplicitTruncationDropsTail) {
  InMemoryRaftLog log;
  ASSERT_TRUE(log.Append({Entry(1, 1), Entry(1, 2), Entry(2, 3)}));
  EXPECT_TRUE(log.TruncateFrom(2));
  EXPECT_EQ(log.LastIndex(), 1u);
  EXPECT_EQ(log.TermAt(2), std::nullopt);
  // A new tail may be appended after truncation.
  EXPECT_TRUE(log.Append({Entry(3, 2)}));
  EXPECT_EQ(log.TermAt(2), std::optional<std::uint64_t>(3));
  EXPECT_TRUE(log.TruncateFrom(9));  // past the end: nothing to drop
  EXPECT_EQ(log.LastIndex(), 2u);
}

TEST(InMemoryRaftLogTest, TruncationBelowFirstIndexIsRefused) {
  InMemoryRaftLog log;
  ASSERT_TRUE(log.Append({Entry(1, 1), Entry(1, 2), Entry(1, 3)}));
  ASSERT_TRUE(log.EvictPrefixTo(2));
  EXPECT_EQ(log.FirstIndex(), 3u);
  EXPECT_FALSE(log.TruncateFrom(2));
  EXPECT_EQ(log.LastIndex(), 3u);
}

TEST(InMemoryRaftLogTest, CrashKeepsOnlyDurableWrites) {
  InMemoryRaftLog log;
  ASSERT_TRUE(log.Append({Entry(1, 1), Entry(1, 2)}));
  const std::uint64_t first = log.WriteSequence();
  ASSERT_TRUE(log.Append({Entry(1, 3)}));
  log.MarkDurableThrough(first);
  EXPECT_EQ(log.DurableLastIndex(), 2u);
  log.TrimToDurable();
  EXPECT_EQ(log.LastIndex(), 2u);
  // A sequence from before the crash marks nothing new.
  ASSERT_TRUE(log.Append({Entry(1, 3)}));
  log.MarkDurableThrough(first + 1);
  log.TrimToDurable();
  EXPECT_EQ(log.LastIndex(), 2u);
}

TEST(InMemoryRaftLogTest, UnsyncedTruncationIsUndoneByACrash) {
  InMemoryRaftLog log;
  ASSERT_TRUE(log.Append({Entry(1, 1), Entry(1, 2), Entry(1, 3)}));
  log.MarkDurableThrough(log.WriteSequence());
  ASSERT_TRUE(log.TruncateFrom(2));
  ASSERT_TRUE(log.Append({Entry(2, 2)}));
  EXPECT_EQ(log.TermAt(2), std::optional<std::uint64_t>(2));
  log.TrimToDurable();
  EXPECT_EQ(log.LastIndex(), 3u);
  EXPECT_EQ(log.TermAt(2), std::optional<std::uint64_t>(1));

  ASSERT_TRUE(log.TruncateFrom(2));
  ASSERT_TRUE(log.Append({Entry(2, 2)}));
  log.MarkDurableThrough(log.WriteSequence());
  log.TrimToDurable();
  EXPECT_EQ(log.LastIndex(), 2u);
  EXPECT_EQ(log.TermAt(2), std::optional<std::uint64_t>(2));
}

TEST(InMemoryRaftLogTest, HardStateRoundTrip) {
  InMemoryRaftLog log;
  RaftHardState state;
  state.term = 7;
  state.votedFor = 2;
  state.commitIndex = 5;
  log.SetHardState(state);
  EXPECT_EQ(log.HardState(), state);
}

}  // namespace
}  // namespace stepraft
