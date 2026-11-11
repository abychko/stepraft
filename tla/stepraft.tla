------------------------------- MODULE stepraft -------------------------------
(* Copyright (c) 2026, Alexey Bychko. GPL-2.0, as the rest of the project. *)
(* The Raft core (RaftNode) at the grain of its API: each action           *)
(* is one call (Tick, Step, Propose, TakeReady, Advance) and does what the *)
(* code does in it. Timer outcomes and the leader's choice of what to      *)
(* append are left open; a trace of the code pins them down. A node's     *)
(* disk keeps what its executor wrote; a crash keeps only the disk and the *)
(* writes that reached it, a restart starts the core from them.            *)
EXTENDS Naturals, Sequences, FiniteSets, Bags, TLC

CONSTANT Server

\* RaftMessageType
PreVoteRequest == 0
PreVoteResponse == 1
VoteRequest == 2
VoteResponse == 3
AppendEntries == 4
AppendEntriesResponse == 5
TimeoutNow == 6
StartAt == 7

\* RaftEntryKind
Payload == 0
NoOp == 1

VARIABLES node, network
vars == <<node, network>>

Max(a, b) == IF a > b THEN a ELSE b
Min(a, b) == IF a < b THEN a ELSE b
MaxOf(S) == CHOOSE x \in S : \A y \in S : y <= x
MinOf(S) == CHOOSE x \in S : \A y \in S : x <= y

Voters == Server
Quorum == Cardinality(Voters) \div 2 + 1

RECURSIVE SeqToBag(_)
SeqToBag(s) == IF s = <<>> THEN EmptyBag
               ELSE SetToBag({Head(s)}) (+) SeqToBag(Tail(s))
BagFilter(B, P(_)) == [m \in {x \in DOMAIN B : P(x)} |-> B[m]]

LastIndex(n) == Len(n.log)
TermAt(n, k) == IF k = 0 THEN 0 ELSE n.log[k].term
LastTerm(n) == TermAt(n, LastIndex(n))

\* Every field a RaftMessage has but the sender's storage, which the
\* spec does not model, at its default.
Msg(type, from, to, t) ==
  [type |-> type, from |-> from, to |-> to, term |-> t, pi |-> 0, pt |-> 0,
   ent |-> <<>>, c |-> 0, tr |-> FALSE, hb |-> FALSE, g |-> FALSE,
   ok |-> FALSE, ci |-> 0, ct |-> 0, mi |-> 0]

\* Sent only once what authorizes them is durable.
Vouches(m) == \/ m.type \in {VoteRequest, VoteResponse, PreVoteResponse}
              \/ m.type = AppendEntriesResponse /\ ~m.hb
Immediate(m) == ~Vouches(m)
IsAppend(m) == m.type = AppendEntries /\ ~m.hb

InitNode ==
  [term |-> 0, vote |-> 0, role |-> "F", pre |-> FALSE, leader |-> 0,
   log |-> <<>>, commit |-> 0, granted |-> {}, rejected |-> {},
   match |-> [j \in Server |-> 0], durable |-> 0,
   \* The open Ready: entries to append, truncation, hard state, sync.
   app |-> <<>>, trunc |-> 0, hsd |-> FALSE, hss |-> FALSE, rhs |-> FALSE,
   rsync |-> FALSE,
   \* Messages sent since the last TakeReady.
   out |-> EmptyBag,
   \* Taken Readies awaiting Advance, oldest first.
   taken |-> <<>>,
   \* Down between a crash and its restart.
   up |-> TRUE,
   \* The disk: log and hard state <<term, vote, commit>>.
   dlog |-> <<>>, dhs |-> <<0, 0, 0>>,
   \* Writes of taken Readies not known to be on the disk, oldest first:
   \* the log as the write leaves it, and the hard state it carries.
   pend |-> <<>>]

Init == /\ node = [i \in Server |-> InitNode]
        /\ network = EmptyBag

----------------------------------------------------------------------------
\* Pieces of RaftNode, each a function of the node record.

Send(n, msgs) == [n EXCEPT !.out = @ (+) msgs]

Flush(n) == IF n.hsd
            THEN [n EXCEPT !.rhs = TRUE, !.rsync = @ \/ n.hss,
                           !.hsd = FALSE, !.hss = FALSE]
            ELSE n

SetCommit(n, k) == IF k <= n.commit THEN n
                   ELSE [n EXCEPT !.commit = k, !.hsd = TRUE]

\* ents: <<index, term, kind>> tuples, contiguous.
AppendToLog(n, ents) ==
  IF ents = <<>> THEN n
  ELSE LET from == ents[1][1]
           n1 == IF from <= LastIndex(n)
                 THEN [n EXCEPT !.trunc = IF @ = 0 THEN from ELSE Min(@, from),
                                !.app = SelectSeq(@, LAMBDA e : e[1] < from),
                                !.durable = Min(@, from - 1),
                                !.log = SubSeq(@, 1, from - 1)]
                 ELSE n
       IN [n1 EXCEPT !.log = @ \o [k \in 1..Len(ents) |->
                                     [term |-> ents[k][2], kind |-> ents[k][3]]],
                     !.app = @ \o ents, !.rsync = TRUE]

BecomeFollower(n, t, l) ==
  LET n1 == IF t # n.term
            THEN [n EXCEPT !.term = t, !.vote = 0, !.hsd = TRUE, !.hss = TRUE]
            ELSE n
  IN [n1 EXCEPT !.role = "F", !.pre = FALSE, !.leader = l, !.granted = {},
                !.rejected = {}]

BecomeLeader(n, i) ==
  LET n1 == [n EXCEPT !.role = "L", !.pre = FALSE, !.leader = i,
                      !.granted = {}, !.rejected = {},
                      !.match = [j \in Server |-> 0]]
  IN AppendToLog(n1, <<<<LastIndex(n1) + 1, n1.term, NoOp>>>>)

Campaign(n, i, preVote, transfer) ==
  IF n.role = "L" \/ i \notin Voters THEN n
  ELSE LET t == n.term + 1
           n1 == [n EXCEPT !.role = "C", !.pre = preVote, !.leader = 0,
                           !.granted = {i}, !.rejected = {}]
           n2 == IF preVote THEN n1
                 ELSE [n1 EXCEPT !.term = t, !.vote = i, !.hsd = TRUE,
                                 !.hss = TRUE]
           type == IF preVote THEN PreVoteRequest ELSE VoteRequest
           reqs == {[Msg(type, i, j, t) EXCEPT !.pi = LastIndex(n),
                       !.pt = LastTerm(n), !.tr = transfer] :
                    j \in Voters \ {i}}
       IN Send(n2, SetToBag(reqs))

Heartbeats(n, i) ==
  {[Msg(AppendEntries, i, j, n.term) EXCEPT !.hb = TRUE, !.pi = n.match[j],
      !.pt = TermAt(n, n.match[j]), !.c = Min(n.commit, n.match[j])] :
   j \in Server \ {i}}

MaybeCommit(n, i) ==
  LET M(v) == IF v = i THEN n.durable ELSE n.match[v]
      c == MaxOf({k \in 0..LastIndex(n) :
                    Cardinality({v \in Voters : M(v) >= k}) >= Quorum})
  IN IF c > n.commit /\ TermAt(n, c) = n.term THEN SetCommit(n, c) ELSE n

\* An append the leader may send now: from its log, past what the
\* follower is known to hold, with its commit point.
AppendOk(n, i, m) ==
  /\ IsAppend(m)
  /\ n.role = "L"
  /\ m.to \in Server \ {i}
  /\ m.pi >= n.match[m.to]
  /\ m.pi + Len(m.ent) <= LastIndex(n)
  /\ m = [Msg(AppendEntries, i, m.to, n.term) EXCEPT !.pi = m.pi,
            !.pt = TermAt(n, m.pi), !.c = n.commit,
            !.ent = [k \in 1..Len(m.ent) |->
                       <<m.pi + k, n.log[m.pi + k].term, n.log[m.pi + k].kind>>]]

----------------------------------------------------------------------------
\* Tick: the timer outcome is open.

TickChoices(n) == IF n.role = "L" THEN {"none", "stepdown", "heartbeat"}
                  ELSE {"none", "campaign"}

TickNode(n, i, c) ==
  CASE c = "none" -> n
    [] c = "stepdown" -> Flush(BecomeFollower(n, n.term, 0))
    [] c = "heartbeat" -> Flush(Send(n, SetToBag(Heartbeats(n, i))))
    [] c = "campaign" -> Flush(Campaign(n, i, TRUE, FALSE))

----------------------------------------------------------------------------
\* Step

HandleVoteRequest(n, i, m) ==
  LET preVote == m.type = PreVoteRequest
      canVote == \/ n.vote = m.from
                 \/ n.vote = 0 /\ n.leader = 0
                 \/ preVote /\ m.term > n.term
      upToDate == \/ m.pt > LastTerm(n)
                  \/ m.pt = LastTerm(n) /\ m.pi >= LastIndex(n)
      ok == canVote /\ upToDate
      rtype == IF preVote THEN PreVoteResponse ELSE VoteResponse
      r == [Msg(rtype, i, m.from, IF ok THEN m.term ELSE n.term)
              EXCEPT !.g = ok]
      n1 == IF ok /\ ~preVote
            THEN [n EXCEPT !.vote = m.from, !.hsd = TRUE, !.hss = TRUE]
            ELSE n
  IN Send(n1, SetToBag({r}))

HandleVoteResponse(n, i, m) ==
  LET preVote == m.type = PreVoteResponse
  IN IF \/ n.role # "C" \/ preVote # n.pre
        \/ preVote /\ m.g /\ m.term # n.term + 1
        \/ m.from \notin Voters
     THEN n
     ELSE LET n1 == IF m.from \in n.granted \cup n.rejected THEN n
                    ELSE IF m.g THEN [n EXCEPT !.granted = @ \cup {m.from}]
                    ELSE [n EXCEPT !.rejected = @ \cup {m.from}]
          IN IF Cardinality(n1.granted) >= Quorum
             THEN IF n1.pre THEN Campaign(n1, i, FALSE, FALSE)
                  ELSE BecomeLeader(n1, i)
             ELSE IF Cardinality(Voters) - Cardinality(n1.rejected) < Quorum
             THEN BecomeFollower(n1, n1.term, 0)
             ELSE n1

\* First index of the conflicting term at or below prev, not below the
\* commit point.
ConflictStart(n, prev, pt) ==
  MinOf({x \in Min(prev, n.commit + 1)..prev :
           \A k \in x..prev : TermAt(n, k) = pt})

FirstFresh(n, ents) ==
  LET bad == {k \in 1..Len(ents) :
                ents[k][1] > LastIndex(n) \/ TermAt(n, ents[k][1]) # ents[k][2]}
  IN IF bad = {} THEN Len(ents) + 1 ELSE MinOf(bad)

HandleAppendEntries(n, i, m) ==
  LET n0 == IF n.role = "C" THEN BecomeFollower(n, m.term, m.from) ELSE n
  IN IF n0.role = "L" THEN n0
     ELSE LET n1 == [n0 EXCEPT !.leader = m.from]
              base == [Msg(AppendEntriesResponse, i, m.from, n1.term)
                         EXCEPT !.pi = m.pi, !.hb = m.hb]
          IN IF m.pi > LastIndex(n1)
             THEN Send(n1, SetToBag({[base EXCEPT !.ci = LastIndex(n1) + 1]}))
             ELSE IF TermAt(n1, m.pi) # m.pt
             THEN LET pt == TermAt(n1, m.pi)
                  IN Send(n1, SetToBag({[base EXCEPT
                            !.ci = ConflictStart(n1, m.pi, pt), !.ct = pt]}))
             ELSE LET fresh == FirstFresh(n1, m.ent)
                      n2 == IF fresh <= Len(m.ent)
                            THEN AppendToLog(n1, SubSeq(m.ent, fresh, Len(m.ent)))
                            ELSE n1
                      lastNew == m.pi + Len(m.ent)
                      n3 == SetCommit(n2, Min(m.c, lastNew))
                  IN Send(n3, SetToBag({[base EXCEPT !.ok = TRUE,
                                                     !.mi = lastNew]}))

\* Reprobing after a refusal or a heartbeat answer only sends appends,
\* which AppendOk covers.
HandleAppendResponse(n, i, m) ==
  IF n.role # "L" \/ m.from \notin Server \ {i} \/ m.hb \/ ~m.ok THEN n
  ELSE MaybeCommit([n EXCEPT !.match[m.from] = Max(@, m.mi)], i)

StepCore(n, i, m) ==
  CASE m.type \in {PreVoteRequest, VoteRequest} -> HandleVoteRequest(n, i, m)
    [] m.type \in {PreVoteResponse, VoteResponse} -> HandleVoteResponse(n, i, m)
    [] m.type = AppendEntries -> HandleAppendEntries(n, i, m)
    [] m.type = AppendEntriesResponse -> HandleAppendResponse(n, i, m)
    [] OTHER -> n

\* lease: a vote request of a higher term arrives while the node has heard
\* from a leader within the minimum election timeout.
LeaseChoices(n, m) ==
  IF m.term > n.term /\ m.type \in {PreVoteRequest, VoteRequest} /\ ~m.tr
     /\ n.leader # 0
  THEN BOOLEAN ELSE {FALSE}

StepNode(n, i, m, lease) ==
  IF m.term > n.term
  THEN IF lease THEN n
       ELSE LET keeps == \/ m.type = PreVoteRequest
                         \/ m.type = PreVoteResponse /\ m.g
                fromLeader == m.type \in {AppendEntries, StartAt}
                n1 == IF keeps THEN n
                      ELSE BecomeFollower(n, m.term,
                                          IF fromLeader THEN m.from ELSE 0)
            IN Flush(StepCore(n1, i, m))
  ELSE IF m.term < n.term
  THEN LET r == IF m.type \in {AppendEntries, StartAt}
                THEN {Msg(AppendEntriesResponse, i, m.from, n.term)}
                ELSE IF m.type = PreVoteRequest
                THEN {Msg(PreVoteResponse, i, m.from, n.term)}
                ELSE {}
       IN Flush(Send(n, SetToBag(r)))
  ELSE Flush(StepCore(n, i, m))

----------------------------------------------------------------------------
\* Propose, TakeReady, Advance

ProposeNode(n) ==
  IF n.role # "L" THEN n
  ELSE AppendToLog(n, <<<<LastIndex(n) + 1, n.term, Payload>>>>)

\* The executor writes what a Ready holds and advances on the disk's word
\* when the Ready demands a sync or waits behind one; else it advances at
\* once and the write lands later, unannounced.
TakeNode(n) ==
  LET persist == n.app # <<>> \/ n.trunc # 0 \/ n.rhs
      pend == IF persist
              THEN Append(n.pend, [log |-> n.log,
                     hs |-> IF n.rhs THEN <<n.term, n.vote, n.commit>> ELSE <<>>])
              ELSE n.pend
  IN [n EXCEPT !.app = <<>>, !.trunc = 0, !.rhs = FALSE, !.rsync = FALSE,
               !.out = EmptyBag, !.pend = pend,
               !.taken = Append(@,
                 [last |-> IF n.app = <<>> THEN 0 ELSE n.app[Len(n.app)][1],
                  lastTerm |-> IF n.app = <<>> THEN 0 ELSE n.app[Len(n.app)][2],
                  sync |-> n.rsync,
                  ondisk |-> n.rsync \/ n.taken # <<>>,
                  \* Writes up to this Ready's own.
                  np |-> Len(pend),
                  after |-> BagFilter(n.out, Vouches)])]

\* The first k pending writes reach the disk.
ApplyWrites(n, k) ==
  IF k = 0 THEN n
  ELSE LET hk == {x \in 1..k : n.pend[x].hs # <<>>}
       IN [n EXCEPT !.dlog = n.pend[k].log,
                    !.dhs = IF hk = {} THEN @ ELSE n.pend[MaxOf(hk)].hs,
                    !.pend = SubSeq(@, k + 1, Len(@)),
                    !.taken = [x \in 1..Len(n.taken) |->
                                 [n.taken[x] EXCEPT !.np = Max(0, @ - k)]]]

AdvanceNode(n, i) ==
  LET done == Head(n.taken)
      n0 == ApplyWrites(n, IF done.ondisk THEN done.np ELSE 0)
      n1 == [n0 EXCEPT !.taken = Tail(@)]
      n2 == IF /\ done.last # 0 /\ done.last <= LastIndex(n1)
               /\ TermAt(n1, done.last) = done.lastTerm /\ done.sync
            THEN [n1 EXCEPT !.durable = Max(@, done.last)]
            ELSE n1
  IN IF n2.role = "L" THEN Flush(MaybeCommit(n2, i)) ELSE n2

----------------------------------------------------------------------------
\* Crash and restart

\* Down with what is on the disk after its first k pending writes, as the
\* core will start from it: whatever else the node held is gone.
CrashNode(n, k) ==
  LET d == ApplyWrites(n, k)
  IN [InitNode EXCEPT !.term = d.dhs[1], !.vote = d.dhs[2],
                      !.commit = d.dhs[3], !.log = d.dlog,
                      !.durable = Len(d.dlog), !.dlog = d.dlog, !.dhs = d.dhs,
                      !.up = FALSE]

CrashChoices(n) == 0..Len(n.pend)

----------------------------------------------------------------------------
\* Safety

Committed(i) == SubSeq(node[i].log, 1, node[i].commit)

ElectionSafety ==
  \A i, j \in Server :
    (node[i].role = "L" /\ node[j].role = "L" /\ node[i].term = node[j].term)
      => i = j

LogMatching ==
  \A i, j \in Server :
    \A k \in 1..Min(LastIndex(node[i]), LastIndex(node[j])) :
      node[i].log[k].term = node[j].log[k].term
        => SubSeq(node[i].log, 1, k) = SubSeq(node[j].log, 1, k)

CommitInLog == \A i \in Server : node[i].commit <= LastIndex(node[i])

DiskCommitInLog == \A i \in Server : node[i].dhs[3] <= Len(node[i].dlog)

\* What any node knows committed is on the disks of a quorum.
DiskTerm(j, k) == IF k <= Len(node[j].dlog) THEN node[j].dlog[k].term ELSE 0
CommittedOnDisks ==
  \A i \in Server : \A k \in 1..node[i].commit :
    Cardinality({j \in Voters : DiskTerm(j, k) = node[i].log[k].term}) >= Quorum

StateMachineSafety ==
  \A i, j \in Server :
    LET c == Min(node[i].commit, node[j].commit)
    IN SubSeq(node[i].log, 1, c) = SubSeq(node[j].log, 1, c)

LeaderCompleteness ==
  \A i, l \in Server :
    node[l].role = "L" =>
      \A k \in 1..node[i].commit :
        node[i].log[k].term < node[l].term =>
          k <= LastIndex(node[l]) /\ node[l].log[k] = node[i].log[k]

Safety == ElectionSafety /\ LogMatching /\ CommitInLog /\ StateMachineSafety
          /\ LeaderCompleteness /\ DiskCommitInLog /\ CommittedOnDisks
=============================================================================
