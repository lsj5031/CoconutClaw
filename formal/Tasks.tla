------------------------------- MODULE Tasks -------------------------------
EXTENDS Naturals, FiniteSets
CONSTANTS N, Cap, Attempts, Unsafe
T == 1..N
R == T \X (1..Attempts)
VARIABLES phase, head, calls, active, cancelled, evidence
vars == <<phase, head, calls, active, cancelled, evidence>>
Current(t) == <<t, head[t]>>
Init == /\ phase = [t \in T |-> "queued"]
        /\ head = [t \in T |-> 1] /\ calls = [r \in R |-> 0]
        /\ active = {} /\ cancelled = {} /\ evidence = {}
Start(t) ==
    /\ (phase[t] = "queued" \/ (Unsafe /\ phase[t] = "unknown"))
    /\ Cardinality(active) < Cap /\ t \notin cancelled
    /\ calls[Current(t)] < 2
    /\ calls' = [calls EXCEPT ![Current(t)] = @ + 1]
    /\ active' = active \cup {Current(t)}
    /\ phase' = [phase EXCEPT ![t] = "running"]
    /\ UNCHANGED <<head, cancelled, evidence>>
Stop(t, outcome) ==
    /\ phase[t] = "running"
    /\ phase' = [phase EXCEPT ![t] =
         IF t \in cancelled /\ outcome = "done" THEN "unknown" ELSE outcome]
    /\ active' = active \ {Current(t)}
    /\ UNCHANGED <<head, calls, cancelled, evidence>>
Cancel(t) ==
    /\ t \notin cancelled
    /\ cancelled' = cancelled \cup {t}
    /\ phase' = [phase EXCEPT ![t] =
         IF @ \in {"queued","approval","dependency"} THEN "cancelled" ELSE @]
    /\ UNCHANGED <<head, calls, active, evidence>>
Resume(t, source) ==
    /\ t \notin cancelled /\ head[t] < Attempts
    /\ (phase[t] = "approval" \/
        (phase[t] = "dependency" /\ source # t /\ phase[source] = "done"
         /\ source \notin cancelled))
    /\ head' = [head EXCEPT ![t] = @ + 1]
    /\ evidence' = evidence \cup {<<t, head[t] + 1>>}
    /\ phase' = [phase EXCEPT ![t] = "queued"]
    /\ UNCHANGED <<calls, active, cancelled>>
Next == \E t \in T :
          Start(t) \/ Cancel(t)
          \/ (\E o \in {"done","unknown","cancelled","approval","dependency"} : Stop(t,o))
          \/ (\E source \in T : Resume(t,source))
Spec == Init /\ [][Next]_vars
TypeOK == /\ phase \in [T -> {"queued","running","done","unknown","cancelled","approval","dependency"}]
          /\ head \in [T -> 1..Attempts] /\ calls \in [R -> 0..2]
          /\ active \subseteq R /\ cancelled \subseteq T /\ evidence \subseteq R
Capacity == Cardinality(active) <= Cap
CurrentOwner == \A r \in active : r = Current(r[1]) /\ phase[r[1]] = "running"
ContinuationCause == \A t \in T : head[t] > 1 => Current(t) \in evidence
OneWorkflowLaunch == \A r \in R : calls[r] <= 1
=============================================================================
