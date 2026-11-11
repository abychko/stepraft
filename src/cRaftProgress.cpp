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

#include <algorithm>

namespace stepraft {

RaftProgress::RaftProgress(std::uint64_t next, std::size_t maxMessages,
                           std::uint64_t maxBytes)
    : next_(next),
      maxMessages_(std::max<std::size_t>(maxMessages, 1)),
      maxBytes_(maxBytes) {}

void RaftProgress::Reset(RaftReplicationState state) {
  state_ = state;
  paused_ = false;
  inflight_.clear();
  inflightBytes_ = 0;
}

void RaftProgress::BecomeProbe() {
  Reset(RaftReplicationState::Probe);
  next_ = match_ + 1;
}

void RaftProgress::BecomeReplicate() {
  Reset(RaftReplicationState::Replicate);
  next_ = match_ + 1;
}

bool RaftProgress::Full() const {
  return inflight_.size() >= maxMessages_ ||
         (maxBytes_ != 0 && inflightBytes_ >= maxBytes_);
}

void RaftProgress::SentEntries(std::size_t entries, std::uint64_t bytes) {
  if (state_ == RaftReplicationState::Replicate) {
    if (entries > 0) {
      next_ += entries;
      inflight_.emplace_back(next_ - 1, bytes);
      inflightBytes_ += bytes;
    }
    paused_ = Full();
    return;
  }
  if (entries > 0) paused_ = true;
}

void RaftProgress::SentStartAt() {
  Reset(RaftReplicationState::Probe);
  paused_ = true;
}

bool RaftProgress::MaybeUpdate(std::uint64_t index) {
  if (index <= match_) return false;
  match_ = index;
  next_ = std::max(next_, index + 1);
  paused_ = false;
  return true;
}

bool RaftProgress::MaybeDecrTo(std::uint64_t rejected, std::uint64_t hint) {
  if (state_ == RaftReplicationState::Replicate) {
    if (rejected <= match_) return false;
    next_ = match_ + 1;
    return true;
  }
  // A probe has one message out; a refusal of any other is stale.
  if (next_ - 1 != rejected) return false;
  next_ = std::max(std::min(rejected, std::max<std::uint64_t>(hint, 1)),
                   match_ + 1);
  paused_ = false;
  return true;
}

void RaftProgress::FreeTo(std::uint64_t index) {
  while (!inflight_.empty() && inflight_.front().first <= index) {
    inflightBytes_ -= inflight_.front().second;
    inflight_.pop_front();
  }
}

bool RaftProgress::ObserveStorage(std::uint64_t storageId, std::uint64_t next) {
  if (storageId == 0 || storageId == storageId_) return false;
  const bool replaced = storageId_ != 0;
  storageId_ = storageId;
  if (!replaced) return false;
  match_ = 0;
  next_ = next;
  Reset(RaftReplicationState::Probe);
  return true;
}

}  // namespace stepraft
