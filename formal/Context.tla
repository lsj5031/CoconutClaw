------------------------------ MODULE Context ------------------------------
EXTENDS Naturals, FiniteSets
CONSTANTS N, Limit, Unsafe
S == 1..N
VARIABLES preference, preferenceEvents, facts, snapshot, native, reused,
          blocker, observed, decisions, decisionCount, sent, cancelled
vars == <<preference,preferenceEvents,facts,snapshot,native,reused,
          blocker,observed,decisions,decisionCount,sent,cancelled>>
Init == /\ preference = [s \in S |-> 0] /\ preferenceEvents = {} /\ facts = {}
        /\ snapshot = [s \in S |-> 0] /\ native = [s \in S |-> 0] /\ reused = {}
        /\ blocker = [s \in S |-> 0] /\ observed = [s \in S |-> 0]
        /\ decisions = {} /\ decisionCount = [s \in S |-> 0] /\ sent = {} /\ cancelled = {}
Preference(s, actor) ==
    /\ actor = s /\ preference[s] < Limit
    /\ preference' = [preference EXCEPT ![s] = @ + 1]
    /\ preferenceEvents' = preferenceEvents \cup {<<s,actor,preference[s]+1>>}
    /\ UNCHANGED <<facts,snapshot,native,reused,blocker,observed,decisions,decisionCount,sent,cancelled>>
Fact(s) == /\ facts' = facts \cup {s}
           /\ UNCHANGED <<preference,preferenceEvents,snapshot,native,reused,blocker,observed,decisions,decisionCount,sent,cancelled>>
Turn(s) == /\ snapshot' = [snapshot EXCEPT ![s] = preference[s]]
           /\ native' = [native EXCEPT ![s] = preference[s]]
           /\ reused' = IF native[s] = preference[s] THEN reused \cup {<<s,native[s],preference[s]>>} ELSE reused
           /\ UNCHANGED <<preference,preferenceEvents,facts,blocker,observed,decisions,decisionCount,sent,cancelled>>
Change(s) == /\ blocker[s] < Limit /\ blocker' = [blocker EXCEPT ![s] = @ + 1]
             /\ UNCHANGED <<preference,preferenceEvents,facts,snapshot,native,reused,observed,decisions,decisionCount,sent,cancelled>>
Observe(s) ==
    /\ blocker[s] # observed[s] /\ decisionCount[s] < 3
    /\ decisions' = decisions \cup {<<s,blocker[s]>>}
    /\ decisionCount' = [decisionCount EXCEPT ![s] = @ + 1]
    /\ observed' = IF Unsafe THEN observed ELSE [observed EXCEPT ![s] = blocker[s]]
    /\ UNCHANGED <<preference,preferenceEvents,facts,snapshot,native,reused,blocker,sent,cancelled>>
Send(s) == /\ <<s,blocker[s]>> \in decisions /\ s \notin cancelled
           /\ sent' = sent \cup {<<s,blocker[s]>>}
           /\ UNCHANGED <<preference,preferenceEvents,facts,snapshot,native,reused,blocker,observed,decisions,decisionCount,cancelled>>
Cancel(s) == /\ cancelled' = cancelled \cup {s}
             /\ UNCHANGED <<preference,preferenceEvents,facts,snapshot,native,reused,blocker,observed,decisions,decisionCount,sent>>
Next == \E s \in S : (\E a \in 0..N : Preference(s,a)) \/ Fact(s) \/ Turn(s)
                      \/ Change(s) \/ Observe(s) \/ Send(s) \/ Cancel(s)
Spec == Init /\ [][Next]_vars
TypeOK == /\ preference \in [S -> 0..2] /\ blocker \in [S -> 0..2]
          /\ facts \subseteq S /\ cancelled \subseteq S /\ decisionCount \in [S -> 0..3]
PreferenceAuthority == \A p \in preferenceEvents : p[1] = p[2]
NativeGeneration == \A r \in reused : r[2] = r[3]
NoticeDedup == \A s \in S : decisionCount[s] = Cardinality({d \in decisions : d[1] = s})
NoInventedNotice == sent \subseteq decisions
=============================================================================
