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
#include <cstddef>
#include <cstdint>
#include <deque>
#include <utility>
#include "stepraft/eRaftReplicationState.hpp"

namespace stepraft {

// What a leader knows about one follower's log and what it has in flight
// to it.
class RaftProgress {
 public:
  RaftProgress() = default;
  // maxBytes 0: no byte limit, only the message count.
  RaftProgress(std::uint64_t next, std::size_t maxMessages,
               std::uint64_t maxBytes);

  RaftReplicationState State() const { return state_; }
  std::uint64_t Match() const { return match_; }
  std::uint64_t Next() const { return next_; }
  // No AppendEntries goes out while paused.
  bool Paused() const { return paused_; }
  std::size_t InflightMessages() const { return inflight_.size(); }
  std::uint64_t InflightBytes() const { return inflightBytes_; }

  void BecomeProbe();
  void BecomeReplicate();
  // An AppendEntries with `entries` entries and `bytes` of payload went
  // out starting at Next().
  void SentEntries(std::size_t entries, std::uint64_t bytes);
  // A StartAt went out: nothing more until an answer or a heartbeat's.
  void SentStartAt();
  // The follower holds the log up to `index`; false if nothing new.
  bool MaybeUpdate(std::uint64_t index);
  // The follower refused entries after `rejected`; `hint` is where it
  // suggests to resume. False when the refusal is stale.
  bool MaybeDecrTo(std::uint64_t rejected, std::uint64_t hint);
  // Messages ending at or before `index` are no longer in flight.
  void FreeTo(std::uint64_t index);
  void Unpause() { paused_ = false; }
  // The follower answered from `storageId` (0: not reported). A storage
  // other than the one seen before is a new log: start over from
  // nothing, with `next` as for a new member. True when that happened.
  bool ObserveStorage(std::uint64_t storageId, std::uint64_t next);
  // The follower's storage as its last answer named it; 0 before one.
  std::uint64_t StorageId() const { return storageId_; }

 private:
  bool Full() const;
  void Reset(RaftReplicationState state);

  RaftReplicationState state_ = RaftReplicationState::Probe;
  std::uint64_t match_ = 0;
  std::uint64_t next_ = 1;
  bool paused_ = false;
  std::size_t maxMessages_ = 1;
  std::uint64_t maxBytes_ = 0;
  // Last index and payload bytes of each message in flight, oldest first.
  std::deque<std::pair<std::uint64_t, std::uint64_t>> inflight_;
  std::uint64_t inflightBytes_ = 0;
  std::uint64_t storageId_ = 0;
};

}  // namespace stepraft
