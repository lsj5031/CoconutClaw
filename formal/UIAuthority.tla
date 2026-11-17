---------------------------- MODULE UIAuthority ----------------------------
EXTENDS Naturals, FiniteSets
CONSTANTS B, Scopes, Observe, Unsafe
T == 1..B
S == 1..Scopes
Owner(s) == 1
Scope(b) == IF Scopes = 2 /\ b = B THEN 2 ELSE 1
VARIABLES issued, expired, allowed, consumed, ready, unknown, calls, uses, reads
vars == <<issued,expired,allowed,consumed,ready,unknown,calls,uses,reads>>
Init == /\ issued = {} /\ expired = {} /\ allowed = S
        /\ consumed = {} /\ ready = {} /\ unknown = {}
        /\ calls = [b \in T |-> 0] /\ uses = {} /\ reads = {}
Authority(b,a,s) == a = Owner(s) /\ s = Scope(b) /\ s \in allowed
Valid(b,a,s) == /\ b \in issued /\ b \notin expired
                /\ (Authority(b,a,s) \/ Unsafe = 1)
                /\ (b \notin consumed \/ Unsafe = 2)
Issue(b) ==
    /\ b \notin issued /\ Scope(b) \in allowed
    /\ issued' = issued \cup {b}
    /\ UNCHANGED <<expired,allowed,consumed,ready,unknown,calls,uses,reads>>
Expire(b) ==
    /\ b \in issued \ expired /\ expired' = expired \cup {b}
    /\ UNCHANGED <<issued,allowed,consumed,ready,unknown,calls,uses,reads>>
Revoke(s) ==
    /\ s \in allowed /\ allowed' = allowed \ {s}
    /\ UNCHANGED <<issued,expired,consumed,ready,unknown,calls,uses,reads>>
Read(b,a,s) ==
    /\ b \in Observe /\ Valid(b,a,s)
    /\ reads' = reads \cup {b}
    /\ uses' = uses \cup {<<b,a,s,Authority(b,a,s)>>}
    /\ UNCHANGED <<issued,expired,allowed,consumed,ready,unknown,calls>>
Consume(b,a,s) ==
    /\ b \notin Observe /\ Valid(b,a,s) /\ b \notin ready /\ calls[b] < 2
    \* Runtime-control consumption is durable before any external invocation.
    /\ consumed' = consumed \cup {b} /\ ready' = ready \cup {b}
    /\ uses' = uses \cup {<<b,a,s,Authority(b,a,s)>>}
    /\ UNCHANGED <<issued,expired,allowed,unknown,calls,reads>>
Invoke(b) ==
    /\ b \in ready /\ calls[b] < 2
    /\ calls' = [calls EXCEPT ![b] = @ + 1] /\ ready' = ready \ {b}
    /\ UNCHANGED <<issued,expired,allowed,consumed,unknown,uses,reads>>
Crash(b) ==
    /\ b \in ready
    /\ ready' = ready \ {b} /\ unknown' = unknown \cup {b}
    /\ UNCHANGED <<issued,expired,allowed,consumed,calls,uses,reads>>
Next == \/ \E b \in T : Issue(b) \/ Expire(b) \/ Invoke(b) \/ Crash(b)
        \/ \E s \in S : Revoke(s)
        \/ \E b \in T, a \in {1,2}, s \in S : Read(b,a,s) \/ Consume(b,a,s)
Spec == Init /\ [][Next]_vars
TypeOK == /\ issued \subseteq T /\ expired \subseteq issued /\ allowed \subseteq S
          /\ consumed \subseteq issued /\ ready \subseteq consumed
          /\ unknown \subseteq consumed /\ calls \in [T -> 0..2]
AuthorityAtUse == \A u \in uses : u[4]
SingleUse == \A b \in T : calls[b] <= 1
ObservationNoRuntime == \A b \in Observe : calls[b] = 0 /\ b \notin consumed
ConsumedBeforeEffect == \A b \in T : calls[b] > 0 => b \in consumed
=============================================================================
