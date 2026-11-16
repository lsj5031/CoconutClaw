------------------------------ MODULE Process ------------------------------
EXTENDS Naturals, FiniteSets
CONSTANTS Limit, Unsafe
VARIABLES phase, claim, token, launches, cancelled, timedout, pipes,
          leader, groupOwned, activity, last, clock, terminal
vars == <<phase,claim,token,launches,cancelled,timedout,pipes,
          leader,groupOwned,activity,last,clock,terminal>>
Init == /\ phase = "idle" /\ claim = FALSE /\ token = FALSE /\ launches = 0
        /\ cancelled = FALSE /\ timedout = FALSE /\ pipes = {} /\ leader = FALSE
        /\ groupOwned = FALSE /\ activity = {} /\ last = 0 /\ clock = 0 /\ terminal = FALSE
Claim == /\ phase = "idle" /\ ~claim
         /\ claim' = TRUE /\ token' = TRUE /\ phase' = "claimed"
         /\ UNCHANGED <<launches,cancelled,timedout,pipes,leader,groupOwned,activity,last,clock,terminal>>
Spawn == /\ phase = "claimed" /\ token /\ ~cancelled
         /\ token' = FALSE /\ phase' = "running" /\ launches' = launches + 1
         /\ groupOwned' = TRUE /\ pipes' = {} /\ leader' = FALSE
         /\ UNCHANGED <<claim,cancelled,timedout,activity,last,clock,terminal>>
Progress(id) ==
    /\ phase = "running" /\ id \notin activity
    /\ activity' = activity \cup {id} /\ last' = clock
    /\ UNCHANGED <<phase,claim,token,launches,cancelled,timedout,pipes,leader,groupOwned,clock,terminal>>
Tick == /\ clock < Limit /\ clock' = clock + 1
        /\ UNCHANGED <<phase,claim,token,launches,cancelled,timedout,pipes,leader,groupOwned,activity,last,terminal>>
Cancel == /\ ~cancelled /\ cancelled' = TRUE
          /\ UNCHANGED <<phase,claim,token,launches,timedout,pipes,leader,groupOwned,activity,last,clock,terminal>>
Timeout == /\ phase = "running" /\ clock - last >= 1
           /\ timedout' = TRUE
           /\ UNCHANGED <<phase,claim,token,launches,cancelled,pipes,leader,groupOwned,activity,last,clock,terminal>>
Exit == /\ phase = "running" /\ leader' = TRUE
        /\ UNCHANGED <<phase,claim,token,launches,cancelled,timedout,pipes,groupOwned,activity,last,clock,terminal>>
Drain(p) == /\ phase = "running" /\ pipes' = pipes \cup {p}
            /\ UNCHANGED <<phase,claim,token,launches,cancelled,timedout,leader,groupOwned,activity,last,clock,terminal>>
Kill == /\ phase = "running" /\ (cancelled \/ timedout)
        /\ leader' = TRUE /\ groupOwned' = FALSE
        /\ UNCHANGED <<phase,claim,token,launches,cancelled,timedout,pipes,activity,last,clock,terminal>>
Join == /\ phase = "running" /\ leader /\ pipes = {1,2,3}
        /\ phase' = "joined" /\ groupOwned' = FALSE /\ terminal' = TRUE
        /\ UNCHANGED <<claim,token,launches,cancelled,timedout,pipes,leader,activity,last,clock>>
Crash == /\ phase \in {"claimed","running"}
         /\ phase' = "unknown" /\ token' = FALSE
         /\ UNCHANGED <<claim,launches,cancelled,timedout,pipes,leader,groupOwned,activity,last,clock,terminal>>
Recover == /\ phase = "unknown"
           /\ phase' = IF Unsafe THEN "claimed" ELSE "stopped"
           /\ token' = Unsafe /\ groupOwned' = FALSE
           /\ UNCHANGED <<claim,launches,cancelled,timedout,pipes,leader,activity,last,clock,terminal>>
Next == Claim \/ Spawn \/ Tick \/ Cancel \/ Timeout \/ Exit \/ Kill \/ Join \/ Crash \/ Recover
        \/ (\E id \in 1..Limit : Progress(id)) \/ (\E p \in {1,2,3} : Drain(p))
Spec == Init /\ [][Next]_vars
TypeOK == /\ phase \in {"idle","claimed","running","joined","unknown","stopped"}
          /\ launches \in 0..2 /\ pipes \subseteq {1,2,3} /\ activity \subseteq 1..Limit
          /\ clock \in 0..Limit /\ last <= clock
ClaimBeforeSpawn == launches > 0 => claim
SingleDispatchToken == launches <= 1
DrainBeforeJoin == terminal => leader /\ pipes = {1,2,3}
=============================================================================
