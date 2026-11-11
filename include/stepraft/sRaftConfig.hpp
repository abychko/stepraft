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
#include <cstddef>
#include <cstdint>
#include <vector>

#include "stepraft/cRaftMembership.hpp"

namespace stepraft {

// Time is logical: every duration is a tick count, and the executor
// decides what one tick is.
struct RaftConfig {
  std::uint32_t selfId = 0;
  // Every node that may be a member: the leader streams its log to all of
  // them, voters or not.
  std::vector<std::uint32_t> members;
  // The voters the cluster starts with, each on its storage; the stored
  // state's own, once it has them, come first.
  std::vector<RaftVoter> voters;
  // Randomized per-election from [min, max] by the core's caller-visible
  // rules; the spread breaks split votes.
  std::uint32_t electionTimeoutMinTicks = 10;
  std::uint32_t electionTimeoutMaxTicks = 20;
  std::uint32_t heartbeatTicks = 1;
  // Liveness holds only with both halves enabled together: pre-vote
  // alone or check-quorum alone re-opens known livelock windows.
  bool preVoteAndCheckQuorum = true;
  // Seeds the core's own deterministic generator that spreads election
  // timeouts. The executor picks it (any source); the same seed and the
  // same events replay the same node.
  std::uint64_t randomSeed = 0;
  // Payload bytes one AppendEntries carries at most (always at least one
  // entry when the follower is behind).
  std::uint64_t maxAppendBytes = 1 << 20;
  // A leader keeps at most this many AppendEntries, and this many payload
  // bytes (0: no byte limit), in flight to one follower.
  std::size_t maxInflightMessages = 256;
  std::uint64_t maxInflightBytes = 4 << 20;
  // Names the storage this member's log lives on, never 0; it changes
  // when the storage is replaced. A leader starts a follower whose
  // storage changed over, as a new member.
  std::uint64_t storageId = 0;

  bool Valid() const {
    for (const RaftVoter &voter : voters)
      if (voter.storageId == 0 ||
          std::find(members.begin(), members.end(), voter.id) == members.end())
        return false;
    return !members.empty() && storageId != 0 &&
           std::find(members.begin(), members.end(), selfId) != members.end() &&
           electionTimeoutMinTicks > 0 &&
           electionTimeoutMinTicks <= electionTimeoutMaxTicks &&
           heartbeatTicks > 0 && heartbeatTicks < electionTimeoutMinTicks &&
           maxInflightMessages > 0;
  }
};

}  // namespace stepraft
