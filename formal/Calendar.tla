------------------------------ MODULE Calendar ------------------------------
EXTENDS Naturals, FiniteSets
CONSTANTS N, FoldSecond, GapNext, SkipMissed, Unsafe, Steady, Stall
S == 1..N
\* Finite oracle: single date -> 1, gap date -> absent/4, fold date -> 6/7.
\* Chrono's real date/offset calculations are an explicit refinement gap.
Slots == {1, IF FoldSecond THEN 7 ELSE 6} \cup (IF GapNext THEN {4} ELSE {})
Future(t) == {x \in Slots : x > t}
Following(t) == IF Future(t) = {} THEN 9 ELSE CHOOSE x \in Future(t) : \A y \in Future(t) : x <= y
VARIABLES now, state, revision, due, blocked, unknown, occurrences, inbox, runs,
          cached, signal, fetching
vars == <<now,state,revision,due,blocked,unknown,occurrences,inbox,runs,cached,signal,fetching>>
Init == /\ now = 0 /\ state = [s \in S |-> "active"]
        /\ revision = [s \in S |-> 1] /\ due = [s \in S |-> 1]
        /\ blocked = {} /\ unknown = {} /\ occurrences = {} /\ inbox = {} /\ runs = {}
        /\ cached = 1 /\ signal = FALSE /\ fetching = TRUE
Tick == /\ now < 8 /\ now' = now + 1
        /\ UNCHANGED <<state,revision,due,blocked,unknown,occurrences,inbox,runs,cached,signal,fetching>>
Mutate(s, target) ==
    /\ ~Steady /\ state[s] \in {"active","paused"}
    /\ state' = [state EXCEPT ![s] = target] /\ signal' = TRUE
    /\ UNCHANGED <<now,revision,due,blocked,unknown,occurrences,inbox,runs,cached,fetching>>
Edit(s) ==
    /\ ~Steady /\ state[s] \in {"active","paused"} /\ revision[s] = 1
    /\ revision' = [revision EXCEPT ![s] = 2]
    /\ due' = [due EXCEPT ![s] = Following(now)] /\ signal' = TRUE
    /\ UNCHANGED <<now,state,blocked,unknown,occurrences,inbox,runs,cached,fetching>>
Observe ==
    LET eligible == {due[s] : s \in {s \in S : state[s] = "active" /\ s \notin blocked}}
    IN /\ cached' = IF eligible = {} THEN 9 ELSE CHOOSE x \in eligible : \A y \in eligible : x <= y
       /\ signal' = FALSE
       /\ UNCHANGED <<now,state,revision,due,blocked,unknown,occurrences,inbox,runs,fetching>>
Admit(s) ==
    /\ state[s] = "active" /\ s \notin blocked /\ due[s] <= now
    /\ (~Stall \/ ~fetching)
    /\ LET past == {x \in Slots : due[s] <= x /\ x <= now}
           latest == CHOOSE x \in past : \A y \in past : x >= y
           key == <<s,revision[s],latest>>
           skip == SkipMissed /\ latest < now
       IN /\ past # {}
          /\ occurrences' = IF skip THEN occurrences ELSE occurrences \cup {key}
          /\ inbox' = IF skip \/ Unsafe THEN inbox ELSE inbox \cup {key}
          /\ runs' = IF skip THEN runs ELSE runs \cup {key}
          /\ blocked' = IF skip THEN blocked ELSE blocked \cup {s}
    /\ due' = [due EXCEPT ![s] = Following(now)] /\ signal' = TRUE
    /\ UNCHANGED <<now,state,revision,unknown,cached,fetching>>
Finish(s, known) ==
    /\ ~Steady /\ s \in blocked /\ s \notin unknown
    /\ blocked' = IF known THEN blocked \ {s} ELSE blocked
    /\ unknown' = IF known THEN unknown ELSE unknown \cup {s}
    /\ signal' = TRUE
    /\ UNCHANGED <<now,state,revision,due,occurrences,inbox,runs,cached,fetching>>
FetchReturn == /\ ~Steady /\ fetching /\ fetching' = FALSE
               /\ UNCHANGED <<now,state,revision,due,blocked,unknown,occurrences,inbox,runs,cached,signal>>
Next == Tick \/ Observe \/ FetchReturn \/ (\E s \in S :
          Admit(s) \/ Edit(s) \/ (\E x \in {"active","paused","cancelled"} : Mutate(s,x))
          \/ (\E known \in BOOLEAN : Finish(s,known)))
Spec == Init /\ [][Next]_vars
LiveSpec == Spec /\ WF_vars(Tick) /\ WF_vars(Observe) /\ (\A s \in S : WF_vars(Admit(s)))
TypeOK == /\ now \in 0..8 /\ due \in [S -> Slots \cup {9}]
          /\ state \in [S -> {"active","paused","cancelled"}]
          /\ revision \in [S -> 1..2] /\ blocked \subseteq S /\ unknown \subseteq blocked
AtomicOccurrence == occurrences = inbox /\ occurrences = runs
CursorForward == \A key \in occurrences : due[key[1]] > key[3]
EventuallyAdmitted == <>(occurrences # {} /\ fetching)
=============================================================================
