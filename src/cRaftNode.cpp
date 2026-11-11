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

#include <algorithm>
#include <cassert>
#include <functional>
#include <utility>

namespace stepraft {

RaftNode::RaftNode(RaftConfig config, const RaftLogView &log,
                   RaftHardState hardState)
    : config_(std::move(config)), log_(log), hardState_(hardState) {
  assert(config_.Valid());
  randomState_ = config_.randomSeed ^ (config_.selfId * 0x9E3779B97F4A7C15ull);
  unstableOffset_ = log_.LastIndex() + 1;
  durableIndex_ = log_.LastIndex();
  // Only what was committed leaves the log, as etcd's newLog assumes.
  if (log_.FirstIndex() > 1)
    hardState_.commitIndex =
        std::max(hardState_.commitIndex, log_.FirstIndex() - 1);
  MembershipAt committed{hardState_.votersIndex, hardState_.voters,
                         hardState_.votersContext};
  if (committed.voters.empty()) committed = {0, config_.voters, {}};
  membership_.push_back(std::move(committed));
  AdoptMembership(log_.MembershipEntries(membership_.front().index));
  ResetElectionTimeout();
}

bool RaftNode::IsVoter() const {
  return RaftMembership::Contains(Voters(), config_.selfId, config_.storageId);
}

void RaftNode::AdoptMembership(const std::vector<RaftLogEntry> &entries) {
  for (const RaftLogEntry &entry : entries) {
    if (entry.kind != RaftEntryKind::Membership) continue;
    // Below the latest known one it is committed, and so already known.
    if (entry.index <= membership_.back().index) continue;
    std::optional<std::vector<RaftVoter>> voters =
        RaftMembership::Decode(entry.payload);
    assert(voters);
    if (voters)
      membership_.push_back({entry.index, std::move(*voters),
                             RaftMembership::Context(entry.payload)});
  }
}

bool RaftNode::CountsFor(const RaftVoter &voter) const {
  if (voter.id == config_.selfId) return voter.storageId == config_.storageId;
  const auto found = progress_.find(voter.id);
  return found != progress_.end() &&
         found->second.StorageId() == voter.storageId;
}

void RaftNode::StepDownIfRemoved() {
  // Once the membership without it commits (Ongaro §4.2.2); a leader an
  // entry not yet committed adds back stays.
  if (role_ != RaftRole::Leader || membership_.size() > 1 ||
      RaftMembership::Contains(membership_.front().voters, config_.selfId,
                               config_.storageId))
    return;
  // The followers learn the commit point before the cluster elects anew.
  BroadcastHeartbeat();
  BecomeFollower(hardState_.term, 0);
}

RaftReady &RaftNode::PendingReady() {
  if (!open_) open_.emplace();
  return *open_;
}

std::uint64_t RaftNode::NextRandom() {
  randomState_ += 0x9E3779B97F4A7C15ull;
  std::uint64_t z = randomState_;
  z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
  z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
  return z ^ (z >> 31);
}

void RaftNode::ResetElectionTimeout() {
  const std::uint32_t span =
      config_.electionTimeoutMaxTicks - config_.electionTimeoutMinTicks + 1;
  electionTimeout_ = config_.electionTimeoutMinTicks +
                     static_cast<std::uint32_t>(NextRandom() % span);
}

std::uint64_t RaftNode::LastIndex() const {
  if (!unstable_.empty()) return unstableOffset_ + unstable_.size() - 1;
  if (pendingBase_) return unstableOffset_ - 1;
  return std::min(log_.LastIndex(), unstableOffset_ - 1);
}

std::uint64_t RaftNode::FirstIndex() const {
  if (pendingBase_) return pendingBase_->index + 1;
  return std::max<std::uint64_t>(log_.FirstIndex(), 1);
}

std::optional<std::uint64_t> RaftNode::TermAt(std::uint64_t index) const {
  if (index == 0) return 0;
  if (pendingBase_ && index < unstableOffset_) {
    if (index == pendingBase_->index) return pendingBase_->term;
    return std::nullopt;
  }
  if (index < unstableOffset_) return log_.TermAt(index);
  if (index - unstableOffset_ < unstable_.size())
    return unstable_[index - unstableOffset_].term;
  return std::nullopt;
}

std::vector<RaftLogEntry> RaftNode::ReadLog(std::uint64_t first,
                                            std::uint64_t last) const {
  std::vector<RaftLogEntry> out;
  if (first > last || last > LastIndex()) return out;
  if (first < unstableOffset_ && pendingBase_) return out;
  if (first < unstableOffset_) {
    const std::uint64_t stableLast = std::min(last, unstableOffset_ - 1);
    out = log_.Read(first, stableLast, config_.maxAppendBytes);
    if (out.empty() || out.back().index < stableLast) return out;
  }
  std::uint64_t bytes = 0;
  for (const RaftLogEntry &entry : out) bytes += entry.payload.size();
  for (std::uint64_t i = std::max(first, unstableOffset_); i <= last; ++i) {
    const RaftLogEntry &entry = unstable_[i - unstableOffset_];
    bytes += entry.payload.size();
    if (!out.empty() && bytes > config_.maxAppendBytes) break;
    out.push_back(entry);
  }
  return out;
}

void RaftNode::AppendToLog(std::vector<RaftLogEntry> entries) {
  if (entries.empty()) return;
  const std::uint64_t from = entries.front().index;
  assert(from >= 1 && from <= LastIndex() + 1);
  RaftReady &ready = PendingReady();
  if (from <= LastIndex()) {
    assert(from > hardState_.commitIndex);
    ready.truncateFrom =
        ready.truncateFrom ? std::min(*ready.truncateFrom, from) : from;
    std::erase_if(ready.entriesToAppend, [from](const RaftLogEntry &entry) {
      return entry.index >= from;
    });
    durableIndex_ = std::min(durableIndex_, from - 1);
    while (membership_.size() > 1 && membership_.back().index >= from)
      membership_.pop_back();
  }
  if (from < unstableOffset_) {
    unstable_.clear();
    unstableOffset_ = from;
  } else {
    unstable_.resize(from - unstableOffset_);
  }
  unstable_.insert(unstable_.end(), entries.begin(), entries.end());
  AdoptMembership(entries);
  ready.entriesToAppend.insert(ready.entriesToAppend.end(),
                               std::make_move_iterator(entries.begin()),
                               std::make_move_iterator(entries.end()));
  ready.mustSync = true;
}

bool RaftNode::InLease() const {
  return config_.preVoteAndCheckQuorum && leaderId_ != 0 &&
         electionElapsed_ < config_.electionTimeoutMinTicks;
}

bool RaftNode::QuorumActive() const {
  std::size_t active = 0;
  for (const RaftVoter &voter : Voters())
    if (CountsFor(voter) &&
        (voter.id == config_.selfId || recentActive_.contains(voter.id)))
      ++active;
  return active >= RaftMembership::Quorum(Voters());
}

void RaftNode::Send(RaftMessage message) {
  message.from = config_.selfId;
  message.storageId = config_.storageId;
  const bool vouches =
      message.type == RaftMessageType::VoteRequest ||
      message.type == RaftMessageType::VoteResponse ||
      message.type == RaftMessageType::PreVoteResponse ||
      (message.type == RaftMessageType::AppendEntriesResponse &&
       !message.heartbeat);
  if (sendObserver_) sendObserver_(message);
  RaftReady &ready = PendingReady();
  (vouches ? ready.messagesAfterSync : ready.messages)
      .push_back(std::move(message));
}

void RaftNode::Flush() {
  if (!hardStateDirty_) return;
  RaftReady &ready = PendingReady();
  ready.hardState = hardState_;
  if (hardStateSync_) ready.mustSync = true;
  hardStateDirty_ = false;
  hardStateSync_ = false;
}

void RaftNode::SetCommit(std::uint64_t index) {
  if (index <= hardState_.commitIndex) return;
  hardState_.commitIndex = index;
  hardStateDirty_ = true;
  std::size_t committed = 0;
  for (std::size_t i = 1; i < membership_.size(); ++i)
    if (membership_[i].index <= index) committed = i;
  if (committed == 0) return;
  membership_.erase(
      membership_.begin(),
      membership_.begin() + static_cast<std::ptrdiff_t>(committed));
  hardState_.voters = membership_.front().voters;
  hardState_.votersIndex = membership_.front().index;
  hardState_.votersContext = membership_.front().context;
}

void RaftNode::Tick() {
  if (role_ == RaftRole::Leader) {
    TickHeartbeat();
  } else {
    TickElection();
  }
  Flush();
}

void RaftNode::TickElection() {
  if (++electionElapsed_ < electionTimeout_) return;
  electionElapsed_ = 0;
  Campaign(config_.preVoteAndCheckQuorum);
}

void RaftNode::TickHeartbeat() {
  ++heartbeatElapsed_;
  if (++electionElapsed_ >= config_.electionTimeoutMinTicks) {
    electionElapsed_ = 0;
    transferee_ = 0;
    if (config_.preVoteAndCheckQuorum) {
      if (!QuorumActive()) {
        BecomeFollower(hardState_.term, 0);
        return;
      }
      recentActive_.clear();
    }
  }
  if (heartbeatElapsed_ >= config_.heartbeatTicks) {
    heartbeatElapsed_ = 0;
    BroadcastHeartbeat();
  }
}

void RaftNode::Campaign(bool preVote, bool transfer) {
  if (role_ == RaftRole::Leader || !IsVoter()) return;
  role_ = RaftRole::Candidate;
  preCandidate_ = preVote;
  leaderId_ = 0;
  votes_.clear();
  electionElapsed_ = 0;
  ResetElectionTimeout();
  const std::uint64_t term = hardState_.term + 1;
  if (!preVote) {
    hardState_.term = term;
    hardState_.votedFor = config_.selfId;
    hardStateDirty_ = true;
    hardStateSync_ = true;
  }
  votes_[config_.selfId] = true;
  // The only voter wins at once, as etcd's campaign polls its own vote.
  if (RaftMembership::Quorum(Voters()) <= 1) {
    if (preVote)
      Campaign(false, transfer);
    else
      BecomeLeader();
    return;
  }
  const std::uint64_t lastIndex = LastIndex();
  const std::uint64_t lastTerm = TermAt(lastIndex).value_or(0);
  for (const RaftVoter &voter : Voters()) {
    const std::uint32_t id = voter.id;
    if (id == config_.selfId) continue;
    RaftMessage request;
    request.type = preVote ? RaftMessageType::PreVoteRequest
                           : RaftMessageType::VoteRequest;
    request.to = id;
    request.term = term;
    request.prevLogIndex = lastIndex;
    request.prevLogTerm = lastTerm;
    request.transfer = transfer;
    Send(std::move(request));
  }
}

void RaftNode::BecomeFollower(std::uint64_t term, std::uint32_t leader) {
  if (term != hardState_.term) {
    hardState_.term = term;
    hardState_.votedFor.reset();
    hardStateDirty_ = true;
    hardStateSync_ = true;
  }
  role_ = RaftRole::Follower;
  preCandidate_ = false;
  leaderId_ = leader;
  votes_.clear();
  recentActive_.clear();
  progress_.clear();
  broadcastPending_ = false;
  refillPending_.clear();
  transferee_ = 0;
  electionElapsed_ = 0;
  heartbeatElapsed_ = 0;
  ResetElectionTimeout();
}

void RaftNode::BecomeLeader() {
  role_ = RaftRole::Leader;
  preCandidate_ = false;
  leaderId_ = config_.selfId;
  votes_.clear();
  recentActive_.clear();
  transferee_ = 0;
  electionElapsed_ = 0;
  heartbeatElapsed_ = 0;
  progress_.clear();
  for (const std::uint32_t id : config_.members) {
    if (id == config_.selfId) continue;
    progress_.emplace(id,
                      RaftProgress(LastIndex() + 1, config_.maxInflightMessages,
                                   config_.maxInflightBytes));
  }
  // Entries of earlier terms commit only together with one of this term.
  RaftLogEntry noOp;
  noOp.term = hardState_.term;
  noOp.index = LastIndex() + 1;
  noOp.kind = RaftEntryKind::NoOp;
  AppendToLog({std::move(noOp)});
  BroadcastAppend();
}

bool RaftNode::SendAppend(std::uint32_t to, bool sendIfEmpty) {
  const auto found = progress_.find(to);
  if (found == progress_.end()) return false;
  RaftProgress &progress = found->second;
  if (progress.Paused()) return false;
  const std::uint64_t prev = progress.Next() - 1;
  const std::optional<std::uint64_t> prevTerm = TermAt(prev);
  // Index 0 has a term on any log, so the start decides, not the term.
  if (!prevTerm || prev + 1 < FirstIndex()) return SendStartAt(to);
  std::vector<RaftLogEntry> entries = ReadLog(progress.Next(), LastIndex());
  if (entries.empty() && !sendIfEmpty) return false;
  std::uint64_t bytes = 0;
  for (const RaftLogEntry &entry : entries) bytes += entry.payload.size();
  progress.SentEntries(entries.size(), bytes);
  RaftMessage append;
  append.type = RaftMessageType::AppendEntries;
  append.to = to;
  append.term = hardState_.term;
  append.prevLogIndex = prev;
  append.prevLogTerm = *prevTerm;
  append.leaderCommit = hardState_.commitIndex;
  append.entries = std::move(entries);
  Send(std::move(append));
  return true;
}

bool RaftNode::SendStartAt(std::uint32_t to) {
  const std::uint64_t first = FirstIndex();
  if (first <= 1) return false;
  const std::optional<std::uint64_t> baseTerm = TermAt(first - 1);
  if (!baseTerm) return false;
  progress_.at(to).SentStartAt();
  RaftMessage message;
  message.type = RaftMessageType::StartAt;
  message.to = to;
  message.term = hardState_.term;
  message.prevLogIndex = first - 1;
  message.prevLogTerm = *baseTerm;
  message.leaderCommit = hardState_.commitIndex;
  RaftLogEntry membership;
  membership.index = membership_.front().index;
  membership.kind = RaftEntryKind::Membership;
  membership.payload = RaftMembership::Encode(membership_.front().voters,
                                              membership_.front().context);
  message.entries.push_back(std::move(membership));
  Send(std::move(message));
  return true;
}

void RaftNode::HandleStartAt(const RaftMessage &message) {
  if (role_ == RaftRole::Candidate) BecomeFollower(message.term, message.from);
  if (role_ == RaftRole::Leader) return;
  electionElapsed_ = 0;
  leaderId_ = message.from;
  const std::uint64_t base = message.prevLogIndex;
  RaftMessage reply;
  reply.type = RaftMessageType::AppendEntriesResponse;
  reply.to = message.from;
  reply.term = hardState_.term;
  reply.prevLogIndex = base;
  reply.success = true;
  reply.matchIndex = base;
  if (TermAt(base) == message.prevLogTerm) {
    SetCommit(std::min(message.leaderCommit, base));
    Send(std::move(reply));
    return;
  }
  // Everything here is older than the leader's log, and what the leader
  // evicted was committed.
  RaftReady &ready = PendingReady();
  ready.resetTo = RaftReady::Base{base, message.prevLogTerm};
  ready.truncateFrom.reset();
  ready.entriesToAppend.clear();
  ready.mustSync = true;
  pendingBase_ = RaftReady::Base{base, message.prevLogTerm};
  unstable_.clear();
  unstableOffset_ = base + 1;
  durableIndex_ = base;
  const std::vector<RaftVoter> known = Voters();
  std::optional<std::vector<RaftVoter>> voters;
  if (message.entries.size() == 1 &&
      message.entries.front().kind == RaftEntryKind::Membership)
    voters = RaftMembership::Decode(message.entries.front().payload);
  assert(voters);
  std::vector<std::uint8_t> context =
      voters ? RaftMembership::Context(message.entries.front().payload)
             : membership_.back().context;
  membership_.clear();
  membership_.push_back({voters ? message.entries.front().index : 0,
                         voters.value_or(known), std::move(context)});
  hardState_.voters = membership_.front().voters;
  hardState_.votersIndex = membership_.front().index;
  hardState_.votersContext = membership_.front().context;
  hardState_.commitIndex = std::max(hardState_.commitIndex, base);
  hardStateDirty_ = true;
  hardStateSync_ = true;
  Send(std::move(reply));
}

void RaftNode::BroadcastAppend() {
  for (const std::uint32_t id : config_.members)
    if (id != config_.selfId) SendAppend(id, true);
}

void RaftNode::SendHeartbeat(std::uint32_t to) {
  const auto found = progress_.find(to);
  if (found == progress_.end()) return;
  const std::uint64_t match = found->second.Match();
  // Positioned at what the follower is known to hold, so it passes the
  // log check without carrying entries, and commits no further than that.
  RaftMessage heartbeat;
  heartbeat.type = RaftMessageType::AppendEntries;
  heartbeat.heartbeat = true;
  heartbeat.to = to;
  heartbeat.term = hardState_.term;
  heartbeat.prevLogIndex = match;
  heartbeat.prevLogTerm = TermAt(match).value_or(0);
  heartbeat.leaderCommit = std::min(hardState_.commitIndex, match);
  Send(std::move(heartbeat));
}

void RaftNode::FlushSends() {
  if (role_ != RaftRole::Leader) {
    broadcastPending_ = false;
    refillPending_.clear();
    return;
  }
  for (const std::uint32_t id : config_.members) {
    if (id == config_.selfId) continue;
    const bool broadcast = broadcastPending_;
    if (!broadcast && !refillPending_.contains(id)) continue;
    // The broadcast carries the commit point even with nothing new.
    if (broadcast && !SendAppend(id, true)) continue;
    while (SendAppend(id, false)) {
    }
  }
  broadcastPending_ = false;
  refillPending_.clear();
}

void RaftNode::BroadcastHeartbeat() {
  for (const std::uint32_t id : config_.members)
    if (id != config_.selfId) SendHeartbeat(id);
}

void RaftNode::SendTimeoutNow(std::uint32_t to) {
  RaftMessage message;
  message.type = RaftMessageType::TimeoutNow;
  message.to = to;
  message.term = hardState_.term;
  Send(std::move(message));
}

void RaftNode::MaybeCommit() {
  std::vector<std::uint64_t> matched;
  for (const RaftVoter &voter : Voters()) {
    if (!CountsFor(voter)) {
      matched.push_back(0);
    } else if (voter.id == config_.selfId) {
      matched.push_back(durableIndex_);
    } else {
      matched.push_back(progress_.at(voter.id).Match());
    }
  }
  std::sort(matched.begin(), matched.end(), std::greater<>());
  const std::uint64_t candidate = matched[RaftMembership::Quorum(Voters()) - 1];
  // Counting replicas commits only entries of the leader's own term.
  if (candidate > hardState_.commitIndex &&
      TermAt(candidate) == hardState_.term)
    SetCommit(candidate);
}

void RaftNode::Step(RaftMessage message) {
  if (message.term > hardState_.term) {
    const bool voteRequest = message.type == RaftMessageType::PreVoteRequest ||
                             message.type == RaftMessageType::VoteRequest;
    if (voteRequest && !message.transfer && InLease()) return;
    // A pre-vote neither asks for nor grants a term change.
    const bool keepsTerm =
        message.type == RaftMessageType::PreVoteRequest ||
        (message.type == RaftMessageType::PreVoteResponse && message.granted);
    const bool fromLeader = message.type == RaftMessageType::AppendEntries ||
                            message.type == RaftMessageType::StartAt;
    if (!keepsTerm) BecomeFollower(message.term, fromLeader ? message.from : 0);
  } else if (message.term < hardState_.term) {
    // Answer a stale leader or pre-candidate with the current term, so it
    // steps down instead of waiting for its own timeout.
    RaftMessage reply;
    reply.to = message.from;
    reply.term = hardState_.term;
    if (config_.preVoteAndCheckQuorum &&
        (message.type == RaftMessageType::AppendEntries ||
         message.type == RaftMessageType::StartAt)) {
      reply.type = RaftMessageType::AppendEntriesResponse;
      Send(std::move(reply));
    } else if (message.type == RaftMessageType::PreVoteRequest) {
      reply.type = RaftMessageType::PreVoteResponse;
      Send(std::move(reply));
    }
    Flush();
    return;
  }

  switch (message.type) {
    case RaftMessageType::PreVoteRequest:
    case RaftMessageType::VoteRequest:
      HandleVoteRequest(message);
      break;
    case RaftMessageType::PreVoteResponse:
    case RaftMessageType::VoteResponse:
      HandleVoteResponse(message);
      break;
    case RaftMessageType::AppendEntries:
      HandleAppendEntries(message);
      break;
    case RaftMessageType::AppendEntriesResponse:
      HandleAppendResponse(message);
      break;
    case RaftMessageType::TimeoutNow:
      if (role_ == RaftRole::Follower) Campaign(false, true);
      break;
    case RaftMessageType::StartAt:
      HandleStartAt(message);
      break;
  }
  Flush();
}

void RaftNode::HandleVoteRequest(const RaftMessage &message) {
  const bool preVote = message.type == RaftMessageType::PreVoteRequest;
  // Whatever this node's own, perhaps older, membership says (Ongaro
  // §4.1); the candidate counts the vote only for the voter on this
  // node's storage, which the answer names.
  const bool canVote = hardState_.votedFor == message.from ||
                       (!hardState_.votedFor && leaderId_ == 0) ||
                       (preVote && message.term > hardState_.term);
  const std::uint64_t lastIndex = LastIndex();
  const std::uint64_t lastTerm = TermAt(lastIndex).value_or(0);
  const bool upToDate =
      message.prevLogTerm > lastTerm ||
      (message.prevLogTerm == lastTerm && message.prevLogIndex >= lastIndex);

  RaftMessage reply;
  reply.type = preVote ? RaftMessageType::PreVoteResponse
                       : RaftMessageType::VoteResponse;
  reply.to = message.from;
  if (canVote && upToDate) {
    reply.term = message.term;
    reply.granted = true;
    if (!preVote) {
      electionElapsed_ = 0;
      hardState_.votedFor = message.from;
      hardStateDirty_ = true;
      hardStateSync_ = true;
    }
  } else {
    reply.term = hardState_.term;
  }
  Send(std::move(reply));
}

void RaftNode::HandleVoteResponse(const RaftMessage &message) {
  if (role_ != RaftRole::Candidate) return;
  const bool preVote = message.type == RaftMessageType::PreVoteResponse;
  if (preVote != preCandidate_) return;
  if (preVote && message.granted && message.term != hardState_.term + 1) return;
  if (!RaftMembership::Contains(Voters(), message.from, message.storageId))
    return;
  votes_.emplace(message.from, message.granted);

  std::size_t granted = 0;
  std::size_t rejected = 0;
  for (const auto &[id, vote] : votes_) (vote ? granted : rejected)++;
  const std::size_t quorum = RaftMembership::Quorum(Voters());
  if (granted >= quorum) {
    if (preCandidate_) {
      Campaign(false);
    } else {
      BecomeLeader();
    }
  } else if (Voters().size() - rejected < quorum) {
    BecomeFollower(hardState_.term, 0);
  }
}

void RaftNode::HandleAppendEntries(RaftMessage &message) {
  if (role_ == RaftRole::Candidate) BecomeFollower(message.term, message.from);
  if (role_ == RaftRole::Leader) return;
  electionElapsed_ = 0;
  leaderId_ = message.from;

  RaftMessage reply;
  reply.type = RaftMessageType::AppendEntriesResponse;
  reply.to = message.from;
  reply.term = hardState_.term;
  reply.prevLogIndex = message.prevLogIndex;
  reply.heartbeat = message.heartbeat;

  const std::uint64_t prev = message.prevLogIndex;
  if (prev > LastIndex()) {
    reply.conflictIndex = LastIndex() + 1;
    Send(std::move(reply));
    return;
  }
  // No term below the log start: an evicted prefix is committed and so
  // matches by definition.
  const std::optional<std::uint64_t> prevTerm = TermAt(prev);
  if (prevTerm && *prevTerm != message.prevLogTerm) {
    // Hint: the first index of the conflicting term, to skip it at once.
    std::uint64_t first = prev;
    while (first > hardState_.commitIndex + 1 && TermAt(first - 1) == prevTerm)
      --first;
    reply.conflictIndex = first;
    reply.conflictTerm = *prevTerm;
    Send(std::move(reply));
    return;
  }

  std::size_t fresh = 0;
  for (; fresh < message.entries.size(); ++fresh) {
    const RaftLogEntry &entry = message.entries[fresh];
    if (entry.index > LastIndex()) break;
    const std::optional<std::uint64_t> have = TermAt(entry.index);
    if (have && *have != entry.term) break;
  }
  if (fresh < message.entries.size())
    AppendToLog(std::vector<RaftLogEntry>(
        std::make_move_iterator(message.entries.begin() +
                                static_cast<std::ptrdiff_t>(fresh)),
        std::make_move_iterator(message.entries.end())));

  const std::uint64_t lastNew = prev + message.entries.size();
  SetCommit(std::min(message.leaderCommit, lastNew));
  reply.success = true;
  reply.matchIndex = lastNew;
  Send(std::move(reply));
}

void RaftNode::HandleAppendResponse(const RaftMessage &message) {
  if (role_ != RaftRole::Leader) return;
  const auto found = progress_.find(message.from);
  if (found == progress_.end()) return;
  recentActive_.insert(message.from);
  RaftProgress &progress = found->second;
  progress.ObserveStorage(message.storageId, LastIndex() + 1);
  if (message.heartbeat) {
    // An answer means the link works again: resume sending, and probe
    // past messages that may have been lost.
    progress.Unpause();
    if (progress.Match() < LastIndex() ||
        progress.State() == RaftReplicationState::Probe)
      SendAppend(message.from, true);
    return;
  }
  if (!message.success) {
    if (progress.MaybeDecrTo(message.prevLogIndex, message.conflictIndex)) {
      if (progress.State() == RaftReplicationState::Replicate)
        progress.BecomeProbe();
      SendAppend(message.from, true);
    }
    return;
  }
  const bool updated = progress.MaybeUpdate(message.matchIndex);
  if (!updated && !(progress.Match() == message.matchIndex &&
                    progress.State() == RaftReplicationState::Probe))
    return;
  if (progress.State() == RaftReplicationState::Probe)
    progress.BecomeReplicate();
  else
    progress.FreeTo(message.matchIndex);
  const std::uint64_t committed = hardState_.commitIndex;
  MaybeCommit();
  if (hardState_.commitIndex > committed) broadcastPending_ = true;
  refillPending_.insert(message.from);
  if (transferee_ == message.from && progress.Match() == LastIndex())
    SendTimeoutNow(message.from);
  StepDownIfRemoved();
}

bool RaftNode::Propose(std::vector<std::uint8_t> payload) {
  if (role_ != RaftRole::Leader || transferee_ != 0) return false;
  RaftLogEntry entry;
  entry.term = hardState_.term;
  entry.index = LastIndex() + 1;
  entry.kind = RaftEntryKind::Payload;
  entry.payload = std::move(payload);
  std::vector<RaftLogEntry> entries;
  entries.push_back(std::move(entry));
  AppendToLog(std::move(entries));
  broadcastPending_ = true;
  return true;
}

void RaftNode::TransferLeadership(std::uint32_t successor) {
  if (role_ != RaftRole::Leader || successor == config_.selfId ||
      !progress_.contains(successor) || transferee_ == successor ||
      !RaftMembership::HasNode(Voters(), successor))
    return;
  transferee_ = successor;
  // The transfer gets one minimum election timeout to complete.
  electionElapsed_ = 0;
  if (progress_[successor].Match() == LastIndex()) {
    SendTimeoutNow(successor);
  } else {
    SendAppend(successor, true);
  }
}

RaftMembershipOutcome RaftNode::ChangeMembership(
    const RaftMembershipChange &change) {
  if (role_ != RaftRole::Leader || transferee_ != 0)
    return RaftMembershipOutcome::NotLeader;
  // One uncommitted membership at a time, and none before an entry of
  // this term commits: a leader may not append a new configuration entry
  // until it has committed an entry from its current term (Ongaro).
  if (membership_.size() > 1 ||
      TermAt(hardState_.commitIndex) != hardState_.term)
    return RaftMembershipOutcome::Busy;
  const RaftVoter &voter = change.voter;
  const bool context = change.kind == RaftMembershipChangeKind::SetContext;
  if (!context && std::find(config_.members.begin(), config_.members.end(),
                            voter.id) == config_.members.end())
    return RaftMembershipOutcome::Invalid;
  std::vector<RaftVoter> next = Voters();
  if (context) {
    // The voters stay.
  } else if (change.kind == RaftMembershipChangeKind::AddVoter) {
    // A node that votes with other storage is removed first (KIP-853).
    if (voter.storageId == 0 || RaftMembership::HasNode(next, voter.id))
      return RaftMembershipOutcome::Invalid;
    if (voter.id == config_.selfId) {
      if (voter.storageId != config_.storageId)
        return RaftMembershipOutcome::NotCaughtUp;
    } else {
      const auto found = progress_.find(voter.id);
      if (found == progress_.end() ||
          found->second.StorageId() != voter.storageId ||
          found->second.Match() < hardState_.commitIndex)
        return RaftMembershipOutcome::NotCaughtUp;
    }
    next.push_back(voter);
  } else {
    const auto found = std::find(next.begin(), next.end(), voter);
    if (found == next.end() || next.size() == 1)
      return RaftMembershipOutcome::Invalid;
    next.erase(found);
  }
  RaftLogEntry entry;
  entry.term = hardState_.term;
  entry.index = LastIndex() + 1;
  entry.kind = RaftEntryKind::Membership;
  const bool carried =
      context || (change.kind == RaftMembershipChangeKind::RemoveVoter &&
                  !change.context.empty());
  entry.payload = RaftMembership::Encode(
      next, carried ? change.context : membership_.back().context);
  std::vector<RaftLogEntry> entries;
  entries.push_back(std::move(entry));
  AppendToLog(std::move(entries));
  broadcastPending_ = true;
  // A membership without this leader counts the others alone.
  MaybeCommit();
  Flush();
  return RaftMembershipOutcome::Accepted;
}

std::uint64_t RaftNode::StorageSeen(std::uint32_t id) const {
  if (id == config_.selfId) return config_.storageId;
  const auto found = progress_.find(id);
  return found == progress_.end() ? 0 : found->second.StorageId();
}

std::optional<RaftMembershipChange> RaftNode::MembershipStep() const {
  if (role_ != RaftRole::Leader) return std::nullopt;
  const std::vector<RaftVoter> &voters = Voters();
  // As KIP-853's auto-join: the voter of a node on other storage goes
  // first; the node comes back as a new voter on its storage once it
  // caught up. One node out at a time: a member that
  // votes on none comes back before the next goes.
  for (const std::uint32_t id : config_.members) {
    if (RaftMembership::HasNode(voters, id)) continue;
    const std::uint64_t seen = StorageSeen(id);
    const auto found = progress_.find(id);
    if (seen != 0 && found != progress_.end() &&
        found->second.Match() >= hardState_.commitIndex)
      return RaftMembershipChange{RaftMembershipChangeKind::AddVoter,
                                  {id, seen}};
  }
  if (voters.size() < config_.members.size()) return std::nullopt;
  for (const RaftVoter &voter : voters) {
    const std::uint64_t seen = StorageSeen(voter.id);
    if (voter.id == config_.selfId || seen == 0 || seen == voter.storageId)
      continue;
    if (CarriedWithout(voter))
      return RaftMembershipChange{RaftMembershipChangeKind::RemoveVoter, voter};
  }
  return std::nullopt;
}

bool RaftNode::CarriedWithout(const RaftVoter &voter) const {
  if (role_ != RaftRole::Leader) return false;
  // Those that answered lately and hold what is committed are the
  // majority of the voters left.
  const auto healthy = [this](const RaftVoter &other) {
    if (other.id == config_.selfId) return true;
    const auto found = progress_.find(other.id);
    return found != progress_.end() && recentActive_.contains(other.id) &&
           found->second.Match() >= hardState_.commitIndex &&
           found->second.StorageId() == other.storageId;
  };
  const std::vector<RaftVoter> &voters = Voters();
  std::size_t carrying = 0;
  for (const RaftVoter &other : voters)
    if (!(other == voter) && healthy(other)) ++carrying;
  const std::size_t left = voters.size() - 1;
  return left > 0 && carrying >= left / 2 + 1;
}

void RaftNode::SetMembers(std::vector<std::uint32_t> members) {
  assert(std::find(members.begin(), members.end(), config_.selfId) !=
         members.end());
  config_.members = std::move(members);
  if (role_ != RaftRole::Leader) return;
  for (const std::uint32_t id : config_.members) {
    if (id == config_.selfId || progress_.contains(id)) continue;
    progress_.emplace(id,
                      RaftProgress(LastIndex() + 1, config_.maxInflightMessages,
                                   config_.maxInflightBytes));
    SendAppend(id, true);
  }
  std::erase_if(progress_, [this](const auto &held) {
    return std::find(config_.members.begin(), config_.members.end(),
                     held.first) == config_.members.end() &&
           !RaftMembership::HasNode(Voters(), held.first);
  });
  Flush();
}

bool RaftNode::HasReady() const {
  if (broadcastPending_ || !refillPending_.empty()) return true;
  return open_ && !open_->Empty();
}

RaftReady RaftNode::TakeReady() {
  assert(HasReady());
  FlushSends();
  RaftReady ready = std::move(PendingReady());
  open_.reset();
  TakenReady taken;
  if (!ready.entriesToAppend.empty()) {
    taken.lastIndex = ready.entriesToAppend.back().index;
    taken.lastTerm = ready.entriesToAppend.back().term;
  }
  taken.mustSync = ready.mustSync;
  taken.reset = ready.resetTo;
  taken_.push_back(taken);
  ready.role = role_;
  ready.leaderId = leaderId_;
  ready.committedTo = hardState_.commitIndex;
  return ready;
}

void RaftNode::Advance() {
  assert(!taken_.empty());
  const TakenReady done = taken_.front();
  taken_.pop_front();
  // The restart is written: log_ holds the base now.
  if (done.reset && pendingBase_ && pendingBase_->index == done.reset->index &&
      pendingBase_->term == done.reset->term && !(open_ && open_->resetTo))
    pendingBase_.reset();
  // Unless a later prescription already replaced it, the written tail
  // now lives in log_, and on disk if the Ready demanded a sync.
  if (done.lastIndex != 0 && TermAt(done.lastIndex) == done.lastTerm) {
    if (done.mustSync) durableIndex_ = std::max(durableIndex_, done.lastIndex);
    if (done.lastIndex >= unstableOffset_) {
      unstable_.erase(
          unstable_.begin(),
          unstable_.begin() + static_cast<std::ptrdiff_t>(done.lastIndex -
                                                          unstableOffset_ + 1));
      unstableOffset_ = done.lastIndex + 1;
    }
  }
  if (role_ == RaftRole::Leader) {
    MaybeCommit();
    StepDownIfRemoved();
    Flush();
  }
}

}  // namespace stepraft
