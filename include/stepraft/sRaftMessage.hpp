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
#include "stepraft/sRaftLogEntry.hpp"

namespace stepraft {

enum class RaftMessageType : std::uint8_t {
  // Pre-vote probe: term is not bumped, a grant promises nothing.
  PreVoteRequest = 0,
  PreVoteResponse = 1,
  VoteRequest = 2,
  VoteResponse = 3,
  // A heartbeat is an AppendEntries with no entries.
  AppendEntries = 4,
  AppendEntriesResponse = 5,
  // Leadership transfer: the leader tells a caught-up successor to start
  // an immediate election (no pre-vote, no waiting for timeout).
  TimeoutNow = 6,
  // The leader's log starts past what the follower holds: restart the log
  // after prevLogIndex (term prevLogTerm), as etcd's snapshot does but
  // with no data. Its one entry carries the leader's committed membership.
  StartAt = 7,
};

// One flat message shape for the whole protocol: which fields are
// meaningful is defined by the type. Messages are data; transport is
// outside the core.
struct RaftMessage {
  RaftMessageType type = RaftMessageType::AppendEntries;
  std::uint32_t from = 0;
  std::uint32_t to = 0;
  // Sender's current term; for PreVoteRequest, the term the sender
  // would campaign with (current + 1).
  std::uint64_t term = 0;

  // Vote/pre-vote requests and AppendEntries: position of the entry
  // preceding `entries` (log matching); for votes, the sender's last.
  std::uint64_t prevLogIndex = 0;
  std::uint64_t prevLogTerm = 0;

  // AppendEntries only.
  std::vector<RaftLogEntry> entries;
  std::uint64_t leaderCommit = 0;

  // VoteRequest sent on a leader's TimeoutNow: it overrides the lease that
  // otherwise makes members ignore elections while they hear a leader.
  bool transfer = false;
  // An AppendEntries that only keeps the leader known and carries the
  // commit point: no entries, positioned at what the follower is known to
  // hold. Its response says so too.
  bool heartbeat = false;
  // The sender's storage (RaftConfig::storageId).
  std::uint64_t storageId = 0;

  // Responses. An AppendEntriesResponse echoes the request's
  // prevLogIndex, so the leader can tell a stale rejection.
  bool granted = false;  // {Pre,}VoteResponse
  bool success = false;  // AppendEntriesResponse
  // On a failed AppendEntriesResponse: a hint for the leader to skip
  // back faster than one index per round trip.
  std::uint64_t conflictIndex = 0;
  std::uint64_t conflictTerm = 0;
  // On success: the last index the follower now matches.
  std::uint64_t matchIndex = 0;
};

}  // namespace stepraft
