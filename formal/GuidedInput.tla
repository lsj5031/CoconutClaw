---------------------------- MODULE GuidedInput ----------------------------
EXTENDS Naturals, FiniteSets
CONSTANTS Codes, Scopes, Unsafe
C == 1..Codes
S == 1..Scopes
VARIABLES current, issued, scope, expired, effects, submissions, cancelled,
          stale, ordinary
vars == <<current,issued,scope,expired,effects,submissions,cancelled,stale,ordinary>>
Init == /\ current = [s \in S |-> 0] /\ issued = {}
        /\ scope = [c \in C |-> 0] /\ expired = {}
        /\ effects = [c \in C |-> 0] /\ submissions = {}
        /\ cancelled = {} /\ stale = {} /\ ordinary = {}
Open(s,c) ==
    /\ c \notin issued
    /\ issued' = issued \cup {c} /\ scope' = [scope EXCEPT ![c] = s]
    /\ current' = [current EXCEPT ![s] = c]
    /\ UNCHANGED <<expired,effects,submissions,cancelled,stale,ordinary>>
Start(s,c) == current[s] = 0 /\ Open(s,c)
Replace(s,c) == current[s] # 0 /\ Open(s,c)
Expire(c) ==
    /\ c \in issued \ expired /\ expired' = expired \cup {c}
    /\ UNCHANGED <<current,issued,scope,effects,submissions,cancelled,stale,ordinary>>
Submit(s,c,explicit,validValue) ==
    /\ c \in issued /\ scope[c] = s /\ current[s] = c
    /\ c \notin expired \/ Unsafe = 3
    /\ explicit \/ Unsafe = 1
    \* One authenticated /input CODE VALUE SQL transaction; invalid data also
    \* consumes the guide, but never contributes a memory/preference effect.
    /\ current' = [current EXCEPT ![s] = 0]
    /\ effects' = [effects EXCEPT ![c] = @ + IF validValue THEN 1 ELSE 0]
    /\ submissions' = submissions \cup {<<c,explicit,c \in expired>>}
    /\ UNCHANGED <<issued,scope,expired,cancelled,stale,ordinary>>
ExpiredInput(s,c) ==
    /\ current[s] = c /\ c \in expired
    /\ current' = [current EXCEPT ![s] = 0]
    /\ UNCHANGED <<issued,scope,expired,effects,submissions,cancelled,stale,ordinary>>
Cancel(s,c) ==
    /\ current[s] = c /\ c \in issued /\ scope[c] = s
    /\ current' = [current EXCEPT ![s] = 0]
    /\ cancelled' = cancelled \cup {<<c,c>>}
    /\ UNCHANGED <<issued,scope,expired,effects,submissions,stale,ordinary>>
StaleCancel(s,c) ==
    /\ c \in issued /\ scope[c] = s /\ current[s] # 0 /\ current[s] # c
    /\ stale' = stale \cup {<<c,current[s]>>}
    /\ current' = IF Unsafe = 2 THEN [current EXCEPT ![s] = 0] ELSE current
    /\ cancelled' = IF Unsafe = 2 THEN cancelled \cup {<<c,current[s]>>} ELSE cancelled
    /\ UNCHANGED <<issued,scope,expired,effects,submissions,ordinary>>
Ordinary(s) ==
    /\ s \notin ordinary /\ ordinary' = ordinary \cup {s}
    /\ UNCHANGED <<current,issued,scope,expired,effects,submissions,cancelled,stale>>
Next == \/ \E c \in C : Expire(c)
        \/ \E s \in S : Ordinary(s)
        \/ \E s \in S, c \in C : Start(s,c) \/ Replace(s,c) \/ ExpiredInput(s,c)
             \/ Cancel(s,c) \/ StaleCancel(s,c)
             \/ (\E explicit, validValue \in BOOLEAN : Submit(s,c,explicit,validValue))
Spec == Init /\ [][Next]_vars
TypeOK == /\ current \in [S -> 0..Codes] /\ scope \in [C -> 0..Scopes]
          /\ issued \subseteq C /\ expired \subseteq issued
          /\ effects \in [C -> 0..2] /\ ordinary \subseteq S
ScopedCurrent == \A s \in S : current[s] # 0 => scope[current[s]] = s
ExplicitInput == \A u \in submissions : u[2]
NoExpiredUse == \A u \in submissions : ~u[3]
SingleApplication == \A c \in C : effects[c] <= 1
CancelBoundToCode == \A pair \in cancelled : pair[1] = pair[2]
=============================================================================
