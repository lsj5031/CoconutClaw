------------------------------ MODULE History ------------------------------
EXTENDS Naturals, FiniteSets
CONSTANTS Limit, Unsafe
Ids == 1..Limit
Kinds == {"source","candidate","promote","assign","start","result","fence"}
Required(k) == IF k = "fence" THEN 0 ELSE IF k \in {"start","result"} THEN 2 ELSE 1
VARIABLES received, prefix, journal, source, candidate, active, assigned, started, result, fenced
vars == <<received,prefix,journal,source,candidate,active,assigned,started,result,fenced>>
Init == /\ received = [i \in Ids |-> << "none", 0 >>] /\ prefix = 0 /\ journal = {}
        /\ source = FALSE /\ candidate = FALSE /\ active = FALSE
        /\ assigned = FALSE /\ started = FALSE /\ result = FALSE /\ fenced = FALSE
Arrival(id,k,actor) ==
    /\ received[id][1] = "none"
    /\ received' = [received EXCEPT ![id] = <<k,actor>>]
    /\ UNCHANGED <<prefix,journal,source,candidate,active,assigned,started,result,fenced>>
Apply ==
    /\ prefix < Limit /\ ~fenced
    /\ LET id == prefix + 1
           k == received[id][1]
           actor == received[id][2]
       IN /\ k \in Kinds /\ (actor = Required(k) \/ Unsafe)
          /\ CASE k = "source" -> ~source
               [] k = "candidate" -> source /\ ~candidate
               [] k = "promote" -> candidate /\ ~active
               [] k = "assign" -> ~assigned
               [] k = "start" -> assigned /\ ~started
               [] k = "result" -> started /\ ~result
               [] k = "fence" -> TRUE
          /\ prefix' = id /\ journal' = journal \cup {<<id,k,actor>>}
          /\ source' = (source \/ k = "source")
          /\ candidate' = (candidate \/ k = "candidate")
          /\ active' = (active \/ k = "promote")
          /\ assigned' = (assigned \/ k = "assign")
          /\ started' = (started \/ k = "start")
          /\ result' = (result \/ k = "result")
          /\ fenced' = (k = "fence")
    /\ UNCHANGED received
Next == Apply \/ (\E id \in Ids, k \in Kinds, actor \in 0..3 : Arrival(id,k,actor))
Spec == Init /\ [][Next]_vars
TypeOK == /\ prefix \in 0..Limit /\ journal \subseteq (Ids \X Kinds \X (0..3))
ContiguousPrefix == {row[1] : row \in journal} = 1..prefix
WriterAuthority == \A row \in journal : row[3] = Required(row[2])
SourceBeforeMemory == candidate => source
AssignmentBeforeResult == result => started /\ assigned
NoReinterpretation == \A row \in journal : <<row[2],row[3]>> = received[row[1]]
=============================================================================
