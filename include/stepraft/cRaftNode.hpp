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
#include <deque>
#include <functional>
#include <map>
#include <optional>
#include <set>
#include <utility>
#include <vector>
#include "stepraft/cRaftLogView.hpp"
#include "stepraft/cRaftMembership.hpp"
#include "stepraft/cRaftProgress.hpp"
#include "stepraft/sRaftConfig.hpp"
#include "stepraft/sRaftHardState.hpp"
#include "stepraft/sRaftLogEntry.hpp"
#include "stepraft/sRaftMessage.hpp"
#include "stepraft/sRaftReady.hpp"
#include "stepraft/sRaftRole.hpp"

namespace stepraft {

// Deterministic Raft automaton: state x event -> actions. No IO, no
// clocks, no threads; the generator that spreads election timeouts is
// seeded from the config, so a node is a pure function of its config
// and its events. Time is logical ticks. The cycle is: feed events
// (Tick/Step/Propose/TransferLeadership), then drain HasReady/TakeReady,
// execute the prescription, and acknowledge with Advance before taking
// the next Ready. Only the leader creates entries.
class RaftNode {
 public:
  // `log` must outlive the node and change only by executing this
  // node's Readies. `hardState` is what the executor recovered from
  // durable storage (default-constructed on first boot).
  RaftNode(RaftConfig config, const RaftLogView &log, RaftHardState hardState);

  RaftNode(const RaftNode &) = delete;
  RaftNode &operator=(const RaftNode &) = delete;

  // One logical tick. Election and heartbeat countdowns live on ticks;
  // what a tick means in wall time is the executor's choice.
  void Tick();

  // Feed one protocol message from another cluster member; entries are
  // moved out of it.
  void Step(RaftMessage message);

  // Leader only: append one replicated unit (opaque source bytes).
  // Returns false when this node is not the leader or is handing
  // leadership over — the caller routes the payload elsewhere or retries.
  bool Propose(std::vector<std::uint8_t> payload);

  // Leader only: bring `successor` up to the leader's log, then make it
  // start an election at once (node maintenance). Abandoned if it does not
  // complete within the minimum election timeout, as it is when the node
  // no longer votes with its storage.
  void TransferLeadership(std::uint32_t successor);

  // Leader only: add or remove one voter (etcd's ConfChange, one voter at
  // a time). The membership takes effect as its entry is appended, and
  // a leader the committed membership leaves out steps down.
  RaftMembershipOutcome ChangeMembership(const RaftMembershipChange &change);
  // Leader only: the next change toward every member voting on the
  // storage it answers from: add a member that votes on none, remove a
  // voter of other storage. nullopt when there is none to make.
  std::optional<RaftMembershipChange> MembershipStep() const;
  // Leader only: whether the voters but `voter` that answered lately and
  // hold what is committed are a majority of them (etcd's strict
  // reconfiguration check).
  bool CarriedWithout(const RaftVoter &voter) const;
  // The nodes that may be members, this one among them, as the cluster now
  // knows them: a leader streams its log to a new one from here on, and
  // forgets one that left unless it still votes.
  void SetMembers(std::vector<std::uint32_t> members);

  // Ready pipeline. TakeReady may be called whenever HasReady(), also
  // while Readies taken before are still being written; Advance
  // acknowledges the oldest taken Ready once its writes are done.
  bool HasReady() const;
  RaftReady TakeReady();
  void Advance();

  // Observability (same values a Ready reports).
  RaftRole Role() const { return role_; }
  std::uint64_t Term() const { return hardState_.term; }
  std::uint64_t CommitIndex() const { return hardState_.commitIndex; }
  std::uint32_t LeaderId() const { return leaderId_; }
  // The log as this node sees it: what is in the log plus entries it has
  // prescribed but not had acknowledged.
  std::uint64_t LastIndex() const;
  // First index the node holds an entry for, its base being the one before.
  std::uint64_t FirstIndex() const;
  std::optional<std::uint64_t> TermAt(std::uint64_t index) const;
  // The latest membership in the log, committed or not.
  const std::vector<RaftVoter> &Voters() const {
    return membership_.back().voters;
  }
  // What the committed membership carries for the cluster.
  const std::vector<std::uint8_t> &Context() const {
    return membership_.front().context;
  }
  // The same of the latest membership, and the entry the committed one
  // came with (0: the one the node started on).
  const std::vector<std::uint8_t> &LatestContext() const {
    return membership_.back().context;
  }
  std::uint64_t CommittedMembershipIndex() const {
    return membership_.front().index;
  }
  const std::vector<RaftVoter> &CommittedVoters() const {
    return membership_.front().voters;
  }
  // Whether this node, with its storage, is one of them; one that is not
  // follows the log and does not campaign.
  bool IsVoter() const;

  // Called with each message as the node sends it, for tracing.
  void ObserveSends(std::function<void(const RaftMessage &)> observer) {
    sendObserver_ = std::move(observer);
  }

 private:
  std::vector<RaftLogEntry> ReadLog(std::uint64_t first,
                                    std::uint64_t last) const;
  // The storage a node answers from, as the leader last saw it; this
  // node's own for itself, 0 when not seen.
  std::uint64_t StorageSeen(std::uint32_t id) const;
  // Prescribe contiguous entries starting at entries.front().index; an
  // index at or below LastIndex() replaces the tail from there.
  void AppendToLog(std::vector<RaftLogEntry> entries);
  void TickElection();
  void TickHeartbeat();
  void Campaign(bool preVote, bool transfer = false);
  void BecomeFollower(std::uint64_t term, std::uint32_t leader);
  void BecomeLeader();
  void HandleVoteRequest(const RaftMessage &message);
  void HandleVoteResponse(const RaftMessage &message);
  void HandleAppendEntries(RaftMessage &message);
  void HandleAppendResponse(const RaftMessage &message);
  void HandleStartAt(const RaftMessage &message);
  // A follower behind the start of this leader's log restarts its own.
  bool SendStartAt(std::uint32_t to);
  // False when nothing went out: paused, or nothing new and !sendIfEmpty.
  bool SendAppend(std::uint32_t to, bool sendIfEmpty);
  void BroadcastAppend();
  void SendHeartbeat(std::uint32_t to);
  void BroadcastHeartbeat();
  // Sends what proposals and acknowledgements asked for since the last
  // Ready, once per follower: everything fed between two Readies shares
  // one AppendEntries instead of one each.
  void FlushSends();
  void SendTimeoutNow(std::uint32_t to);
  void MaybeCommit();
  void SetCommit(std::uint64_t index);
  // Membership entries among those just appended take effect.
  void AdoptMembership(const std::vector<RaftLogEntry> &entries);
  // Whether the answers of `id` count for the voter it is listed as.
  bool CountsFor(const RaftVoter &voter) const;
  // A leader the committed membership leaves out hands the cluster back.
  void StepDownIfRemoved();
  bool InLease() const;
  bool QuorumActive() const;
  void ResetElectionTimeout();
  std::uint64_t NextRandom();
  void Send(RaftMessage message);
  void Flush();
  RaftReady &PendingReady();

  RaftConfig config_;
  const RaftLogView &log_;
  RaftHardState hardState_;
  RaftRole role_ = RaftRole::Follower;
  // Inside Candidate: probing with pre-votes, term not yet bumped.
  bool preCandidate_ = false;
  std::uint32_t leaderId_ = 0;
  std::uint32_t electionElapsed_ = 0;
  std::uint32_t heartbeatElapsed_ = 0;
  // Randomized in [min, max] each time the countdown restarts.
  std::uint32_t electionTimeout_ = 0;
  std::uint64_t randomState_ = 0;
  // Votes of the current campaign; the first answer of a node counts.
  std::map<std::uint32_t, bool> votes_;
  // Leader: members heard from since the last quorum check.
  std::set<std::uint32_t> recentActive_;

  // The log view: log_ below unstableOffset_, unstable_ from it on.
  std::uint64_t unstableOffset_ = 1;
  std::vector<RaftLogEntry> unstable_;
  // Last index known to be on this node's disk; a leader counts itself
  // towards a commit only up to here.
  std::uint64_t durableIndex_ = 0;

  // Leader: per follower, what it holds and what is in flight to it.
  std::map<std::uint32_t, RaftProgress> progress_;
  std::uint32_t transferee_ = 0;
  // Leader: every follower gets what is new at the next Ready; and
  // followers whose acknowledgements opened room in their window.
  bool broadcastPending_ = false;
  std::set<std::uint32_t> refillPending_;

  bool hardStateDirty_ = false;
  // Term and vote changes must be durable before any message they
  // authorize; a commit index change need not.
  bool hardStateSync_ = false;
  // What Advance needs of each taken Ready, oldest first.
  struct TakenReady {
    // Of the last entry it writes; 0 when it writes none.
    std::uint64_t lastIndex = 0;
    std::uint64_t lastTerm = 0;
    bool mustSync = false;
    std::optional<RaftReady::Base> reset;
  };
  std::deque<TakenReady> taken_;
  // A restart of the log taken or prescribed but not acknowledged: until
  // then log_ still holds what is gone.
  std::optional<RaftReady::Base> pendingBase_;
  // Collects what events prescribe until it is taken.
  std::optional<RaftReady> open_;
  // The committed membership first, then each membership entry above the
  // commit point (KRaft's voter set history): a truncated entry takes
  // its membership with it.
  struct MembershipAt {
    std::uint64_t index = 0;
    std::vector<RaftVoter> voters;
    std::vector<std::uint8_t> context;
  };
  std::vector<MembershipAt> membership_;
  std::function<void(const RaftMessage &)> sendObserver_;
};

}  // namespace stepraft
