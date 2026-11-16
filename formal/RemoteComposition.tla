------------------------- MODULE RemoteComposition -------------------------
EXTENDS Naturals
CONSTANTS Epochs, Unsafe
VARIABLES epoch, reserved, phase, runnerClaim, localCalls, remoteCalls, signed,
          custody, result, outbox, cancel, partition, route, boundRoute, quarantine
vars == <<epoch,reserved,phase,runnerClaim,localCalls,remoteCalls,signed,
          custody,result,outbox,cancel,partition,route,boundRoute,quarantine>>
Init == /\ epoch = 1 /\ reserved = FALSE /\ phase = "queued"
        /\ runnerClaim = FALSE /\ localCalls = 0 /\ remoteCalls = 0 /\ signed = 0
        /\ custody = FALSE /\ result = "none" /\ outbox = FALSE
        /\ cancel = FALSE /\ partition = FALSE /\ route = 1 /\ boundRoute = 1 /\ quarantine = FALSE
Reserve == /\ phase = "queued" /\ ~reserved /\ localCalls = 0
           /\ reserved' = TRUE /\ phase' = "remote"
           /\ UNCHANGED <<epoch,runnerClaim,localCalls,remoteCalls,signed,custody,result,outbox,cancel,partition,route,boundRoute,quarantine>>
LocalStart == /\ (phase = "queued" \/ (Unsafe /\ phase = "remote"))
              /\ (~reserved \/ Unsafe) /\ localCalls = 0 /\ ~cancel
              /\ localCalls' = 1
              /\ UNCHANGED <<epoch,reserved,phase,runnerClaim,remoteCalls,signed,custody,result,outbox,cancel,partition,route,boundRoute,quarantine>>
RunnerStart ==
    /\ reserved /\ phase = "remote" /\ ~runnerClaim /\ epoch = 1 /\ ~cancel
    /\ runnerClaim' = TRUE /\ remoteCalls' = remoteCalls + 1
    /\ UNCHANGED <<epoch,reserved,phase,localCalls,signed,custody,result,outbox,cancel,partition,route,boundRoute,quarantine>>
Sign == /\ runnerClaim /\ signed = 0
        /\ signed' = 1
        /\ UNCHANGED <<epoch,reserved,phase,runnerClaim,localCalls,remoteCalls,custody,result,outbox,cancel,partition,route,boundRoute,quarantine>>
Receive ==
    /\ signed = 1 /\ ~partition /\ ~custody /\ epoch = 1
    /\ custody' = TRUE
    /\ UNCHANGED <<epoch,reserved,phase,runnerClaim,localCalls,remoteCalls,signed,result,outbox,cancel,partition,route,boundRoute,quarantine>>
Apply ==
    /\ custody /\ result \in {"none","unknown"} /\ ~outbox
    /\ IF route = boundRoute
       THEN /\ result' = IF cancel \/ result = "unknown" THEN "unknown" ELSE "completed"
            /\ outbox' = TRUE /\ quarantine' = quarantine
       ELSE /\ quarantine' = TRUE /\ UNCHANGED <<result,outbox>>
    /\ UNCHANGED <<epoch,reserved,phase,runnerClaim,localCalls,remoteCalls,signed,custody,cancel,partition,route,boundRoute>>
Cancel == /\ ~cancel /\ cancel' = TRUE
          /\ UNCHANGED <<epoch,reserved,phase,runnerClaim,localCalls,remoteCalls,signed,custody,result,outbox,partition,route,boundRoute,quarantine>>
Partition == /\ partition' = ~partition
             /\ UNCHANGED <<epoch,reserved,phase,runnerClaim,localCalls,remoteCalls,signed,custody,result,outbox,cancel,route,boundRoute,quarantine>>
Fence == /\ epoch < Epochs /\ epoch' = epoch + 1
         /\ UNCHANGED <<reserved,phase,runnerClaim,localCalls,remoteCalls,signed,custody,result,outbox,cancel,partition,route,boundRoute,quarantine>>
Substitute == /\ result = "none" /\ route = boundRoute /\ route' = 2
              /\ UNCHANGED <<epoch,reserved,phase,runnerClaim,localCalls,remoteCalls,signed,custody,result,outbox,cancel,partition,boundRoute,quarantine>>
Recover ==
    /\ runnerClaim /\ signed = 0 /\ result = "none"
    /\ result' = "unknown"
    /\ UNCHANGED <<epoch,reserved,phase,runnerClaim,localCalls,remoteCalls,signed,custody,outbox,cancel,partition,route,boundRoute,quarantine>>
Next == Reserve \/ LocalStart \/ RunnerStart \/ Sign \/ Receive \/ Apply
        \/ Cancel \/ Partition \/ Fence \/ Substitute \/ Recover
Spec == Init /\ [][Next]_vars
TypeOK == /\ epoch \in 1..Epochs /\ result \in {"none","unknown","completed"}
          /\ localCalls \in 0..1 /\ remoteCalls \in 0..1 /\ signed \in 0..1
SingleExecutionOwner == localCalls + remoteCalls <= 1
AuthenticatedOutbox == outbox => custody /\ signed = 1 /\ route = boundRoute
NoUnknownCompletion == result = "completed" => custody
=============================================================================
