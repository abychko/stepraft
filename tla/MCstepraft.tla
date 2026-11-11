------------------------------ MODULE MCstepraft ------------------------------
(* Copyright (c) 2026, Alexey Bychko. GPL-2.0, as the rest of the project. *)
(* Model checking of stepraft itself on a bounded model: every call in any   *)
(* order, a leader appending whatever it may, a network that loses and     *)
(* duplicates.                                                             *)
EXTENDS stepraft

CONSTANTS MaxTerm, MaxLog, MaxNet, MaxOut, MaxTaken, Duplicates

MCTick(i) ==
  /\ \E c \in TickChoices(node[i]) \ {"none"} :
       node' = [node EXCEPT ![i] = TickNode(node[i], i, c)]
  /\ UNCHANGED network

MCStep(m) ==
  /\ \E lease \in LeaseChoices(node[m.to], m) :
       node' = [node EXCEPT ![m.to] = StepNode(node[m.to], m.to, m, lease)]
  /\ network' = network (-) SetToBag({m})

MCPropose(i) ==
  /\ node[i].role = "L"
  /\ node' = [node EXCEPT ![i] = ProposeNode(node[i])]
  /\ UNCHANGED network

MCAppend(i, j) ==
  LET n == node[i]
  IN /\ n.role = "L"
     /\ \E prev \in n.match[j]..LastIndex(n) :
        \E k \in 0..(LastIndex(n) - prev) :
          LET m == [Msg(AppendEntries, i, j, n.term) EXCEPT !.pi = prev,
                      !.pt = TermAt(n, prev), !.c = n.commit,
                      !.ent = [x \in 1..k |->
                                 <<prev + x, n.log[prev + x].term,
                                   n.log[prev + x].kind>>]]
          IN node' = [node EXCEPT ![i] = Send(n, SetToBag({m}))]
     /\ UNCHANGED network

MCTake(i) ==
  LET n == node[i]
  IN /\ n.out # EmptyBag \/ n.app # <<>> \/ n.trunc # 0 \/ n.rhs
     /\ node' = [node EXCEPT ![i] = TakeNode(n)]
     /\ network' = network (+) BagFilter(n.out, Immediate)

MCAdvance(i) ==
  /\ node[i].taken # <<>>
  /\ node' = [node EXCEPT ![i] = AdvanceNode(node[i], i)]
  /\ network' = network (+) Head(node[i].taken).after

MCDrop(m) ==
  /\ network' = network (-) SetToBag({m})
  /\ UNCHANGED node

MCDuplicate(m) ==
  /\ Duplicates
  /\ network[m] = 1
  /\ network' = network (+) SetToBag({m})
  /\ UNCHANGED node

MCNext ==
  \/ \E i \in Server : MCTick(i) \/ MCPropose(i) \/ MCTake(i) \/ MCAdvance(i)
  \/ \E i, j \in Server : i # j /\ MCAppend(i, j)
  \/ \E m \in DOMAIN network : MCStep(m) \/ MCDrop(m) \/ MCDuplicate(m)

MCSpec == Init /\ [][MCNext]_vars

MCConstraint ==
  /\ BagCardinality(network) <= MaxNet
  /\ \A i \in Server :
       /\ node[i].term <= MaxTerm
       /\ LastIndex(node[i]) <= MaxLog
       /\ BagCardinality(node[i].out) <= MaxOut
       /\ Len(node[i].taken) <= MaxTaken
       /\ Len(node[i].pend) <= MaxTaken + 1

MCSymmetry == Permutations(Server)
=============================================================================
