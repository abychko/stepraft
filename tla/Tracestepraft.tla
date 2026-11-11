----------------------------- MODULE Tracestepraft -----------------------------
(* Copyright (c) 2026, Alexey Bychko. GPL-2.0, as the rest of the project. *)
(* Trace validation: a simulator run, written by raft_trace as NDJSON, is  *)
(* replayed line by line through stepraft. Each line names one call into a   *)
(* core with the node's state after it and the messages it sent; the       *)
(* line matches only if stepraft allows that call to end so; the trace is   *)
(* accepted when some behaviour matches every line.                        *)
EXTENDS stepraft, Json, IOUtils

Trace == TLCEval(ndJsonDeserialize(IOEnv.TRACE))
\* From the first line alone: a set built over all of Trace while the
\* config is read parses the file once per element.
TraceServer == TLCEval(LET s == Trace[1].nodes IN {s[k] : k \in 1..Len(s)})

VARIABLE l
line == Trace[l]

StateOk(n, s) ==
  /\ s.role = n.role
  /\ s.term = n.term
  /\ s.commit = n.commit
  /\ s.leader = n.leader
  /\ s.first = 1
  /\ s.log = [k \in 1..Len(n.log) |-> n.log[k].term]

\* What the call sent: the replies and requests exactly as stepraft computes
\* them, and appends it allows.
Exact(m) == ~IsAppend(m)

OutOk(i, n0, n1, out) ==
  /\ SeqToBag(SelectSeq(out, Exact)) = n1.out (-) n0.out
  /\ \A k \in 1..Len(out) : IsAppend(out[k]) => AppendOk(n1, i, out[k])

WithAppends(n, out) == Send(n, SeqToBag(SelectSeq(out, IsAppend)))

Called(i, n1) ==
  /\ node[i].up
  /\ StateOk(n1, line.s)
  /\ OutOk(i, node[i], n1, line.out)
  /\ node' = [node EXCEPT ![i] = WithAppends(n1, line.out)]

TraceTick ==
  /\ line.e = "tick"
  /\ \E c \in TickChoices(node[line.n]) :
       Called(line.n, TickNode(node[line.n], line.n, c))
  /\ UNCHANGED network

TraceStep ==
  /\ line.e = "step"
  /\ BagIn(line.m, network)
  /\ line.m.to = line.n
  /\ \E lease \in LeaseChoices(node[line.n], line.m) :
       Called(line.n, StepNode(node[line.n], line.n, line.m, lease))
  /\ network' = network (-) SetToBag({line.m})

TracePropose ==
  /\ line.e = "propose"
  /\ line.ok = (node[line.n].role = "L")
  /\ Called(line.n, ProposeNode(node[line.n]))
  /\ UNCHANGED network

ReadyOk(n, r) ==
  /\ r.app = n.app
  /\ r.trunc = n.trunc
  /\ r.reset = <<>>
  /\ r.hs = IF n.rhs THEN <<n.term, n.vote, n.commit>> ELSE <<>>
  /\ r.sync = n.rsync

\* TakeReady sends the appends the Ready's calls asked for.
TraceTake ==
  /\ line.e = "take"
  /\ LET i == line.n
         n1 == WithAppends(node[i], line.out)
     IN /\ node[i].up
        /\ \A k \in 1..Len(line.out) :
             IsAppend(line.out[k]) /\ AppendOk(node[i], i, line.out[k])
        /\ ReadyOk(n1, line.r)
        /\ StateOk(n1, line.s)
        /\ node' = [node EXCEPT ![i] = TakeNode(n1)]
        /\ network' = network (+) BagFilter(n1.out, Immediate)

\* What the oldest Ready held back until durable leaves with its Advance.
TraceAdvance ==
  /\ line.e = "advance"
  /\ node[line.n].taken # <<>>
  /\ Called(line.n, AdvanceNode(node[line.n], line.n))
  /\ network' = network (+) Head(node[line.n].taken).after

\* Which pending writes reached the disk shows only in the restart.
TraceCrash ==
  /\ line.e = "crash"
  /\ node[line.n].up
  /\ \E k \in CrashChoices(node[line.n]) :
       node' = [node EXCEPT ![line.n] = CrashNode(node[line.n], k)]
  /\ UNCHANGED network

TraceRestart ==
  /\ line.e = "restart"
  /\ ~node[line.n].up
  /\ StateOk(node[line.n], line.s)
  /\ node' = [node EXCEPT ![line.n].up = TRUE]
  /\ UNCHANGED network

TraceDrop ==
  /\ line.e = "drop"
  /\ BagIn(line.m, network)
  /\ network' = network (-) SetToBag({line.m})
  /\ UNCHANGED node

TraceBegin ==
  /\ line.e = "init"
  /\ l = 1
  /\ UNCHANGED vars

TraceDuplicate ==
  /\ line.e = "dup"
  /\ BagIn(line.m, network)
  /\ network' = network (+) SetToBag({line.m})
  /\ UNCHANGED node

TraceInit == Init /\ l = 1

\* Some behaviour matched every line: a line no branch matches ends the
\* search short of it, and the depth reached names it. Branches that die
\* later, as a crash's guess at its pending writes does, are no failure.
TraceAccepted ==
  LET d == TLCGet("stats").diameter
  IN d > Len(Trace) \/ (PrintT(<<"unmatched line", d>>) /\ FALSE)

TraceNext ==
  \/ /\ l <= Len(Trace)
     /\ \/ TraceBegin \/ TraceTick \/ TraceStep \/ TracePropose \/ TraceTake
        \/ TraceAdvance \/ TraceDrop \/ TraceDuplicate \/ TraceCrash
        \/ TraceRestart
     /\ l' = l + 1
  \/ /\ l > Len(Trace)
     /\ UNCHANGED <<vars, l>>

=============================================================================
