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
#include <optional>
#include <vector>
#include "stepraft/sRaftHardState.hpp"
#include "stepraft/sRaftLogEntry.hpp"
#include "stepraft/sRaftMessage.hpp"
#include "stepraft/sRaftRole.hpp"

namespace stepraft {

// The core's entire output: a declarative IO prescription. The executor
// (a test simulator or the embedding program) must:
//   1. execute `truncateFrom`, then write `entriesToAppend` and
//      `hardState`; when `mustSync` is set, make all of it durable
//      (fsync);
//   2. send `messages` at any time, before step 1 is done too;
//   3. send `messagesAfterSync` only once step 1 is done, and done for
//      every Ready taken before this one;
//   4. call RaftNode::Advance() once step 1 is done. Readies may be
//      taken while earlier ones are still being written; they are
//      written, and acknowledged, in the order they were taken.
// "Persist before confirming" lives in the contract, not in executor
// discipline: what vouches for this node's disk (a vote, an acknowledged
// append) is in `messagesAfterSync`, and the Ready that makes it true
// demands the sync itself.
struct RaftReady {
  // Set when (term, votedFor, commitIndex) changed.
  std::optional<RaftHardState> hardState;
  std::vector<RaftLogEntry> entriesToAppend;
  // Explicit truncation: entries from this index on (inclusive) are
  // cancelled. Physical truncation is the log implementation's job.
  std::optional<std::uint64_t> truncateFrom;
  // Before anything else: the log restarts after this entry, which it
  // holds as its base; every entry it had goes.
  struct Base {
    std::uint64_t index = 0;
    std::uint64_t term = 0;
  };
  std::optional<Base> resetTo;
  // Nothing in them depends on this node's disk: the leader's appends
  // and heartbeats (a leader writes its own log in parallel with its
  // followers and counts itself once its write is durable), answers to
  // heartbeats, pre-vote requests, TimeoutNow.
  std::vector<RaftMessage> messages;
  // Acknowledged appends, votes and vote requests (a candidate counts its
  // own vote at once), and answers to pre-votes, as etcd-raft holds them
  // back with asynchronous storage writes.
  std::vector<RaftMessage> messagesAfterSync;
  bool mustSync = false;

  // Observability signals (not prescriptions): current role, the leader
  // this node believes in (0 = unknown), and the commit bound readers
  // may be served up to. Entries below that bound may still be on their
  // way to this node's log, in this Ready or an earlier one.
  RaftRole role = RaftRole::Follower;
  std::uint32_t leaderId = 0;
  std::uint64_t committedTo = 0;

  bool Empty() const {
    return !hardState && entriesToAppend.empty() && !truncateFrom && !resetTo &&
           messages.empty() && messagesAfterSync.empty();
  }
};

}  // namespace stepraft
