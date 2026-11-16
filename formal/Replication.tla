---------------------------- MODULE Replication ----------------------------
EXTENDS Naturals, FiniteSets
CONSTANTS N, Generations, Unsafe
Ids == 1..N
VARIABLES generation, highwater, member, time, deadline, connection, messages,
          network, receipts, projection, disclosed, quarantine, checkpoint, fenced
vars == <<generation,highwater,member,time,deadline,connection,messages,
          network,receipts,projection,disclosed,quarantine,checkpoint,fenced>>
Init == /\ generation = 1 /\ highwater = 1 /\ member = TRUE
        /\ time = 0 /\ deadline = 2 /\ connection = 0 /\ messages = {}
        /\ network = TRUE /\ receipts = {} /\ projection = {} /\ disclosed = {}
        /\ quarantine = FALSE /\ checkpoint = FALSE /\ fenced = FALSE
Policy(allow) ==
    /\ generation < Generations /\ ~quarantine
    /\ generation' = generation + 1 /\ highwater' = generation + 1
    /\ member' = allow /\ connection' = 0 /\ deadline' = time + 2
    /\ UNCHANGED <<time,messages,network,receipts,projection,disclosed,quarantine,checkpoint,fenced>>
Tick == /\ time < 3 /\ time' = time + 1
        /\ UNCHANGED <<generation,highwater,member,deadline,connection,messages,network,receipts,projection,disclosed,quarantine,checkpoint,fenced>>
Open == /\ member /\ time < deadline /\ ~quarantine /\ network
        /\ connection' = generation
        /\ UNCHANGED <<generation,highwater,member,time,deadline,messages,network,receipts,projection,disclosed,quarantine,checkpoint,fenced>>
Request(id) ==
    /\ connection > 0
    /\ messages' = messages \cup {<<id,connection>>}
    /\ UNCHANGED <<generation,highwater,member,time,deadline,connection,network,receipts,projection,disclosed,quarantine,checkpoint,fenced>>
Receive(msg) ==
    /\ msg \in messages /\ network /\ ~quarantine /\ ~fenced
    /\ member /\ time < deadline /\ (msg[2] = generation \/ Unsafe)
    /\ msg[1] \notin receipts
    /\ receipts' = receipts \cup {msg[1]}
    /\ disclosed' = disclosed \cup {<<msg[1],msg[2],generation,time,deadline>>}
    /\ UNCHANGED <<generation,highwater,member,time,deadline,connection,messages,network,projection,quarantine,checkpoint,fenced>>
Project(id) ==
    /\ id \in receipts
    /\ projection' = projection \cup {id}
    /\ UNCHANGED <<generation,highwater,member,time,deadline,connection,messages,network,receipts,disclosed,quarantine,checkpoint,fenced>>
Partition == /\ network' = ~network
             /\ UNCHANGED <<generation,highwater,member,time,deadline,connection,messages,receipts,projection,disclosed,quarantine,checkpoint,fenced>>
RestoreLatch ==
    /\ quarantine' = TRUE /\ checkpoint' = FALSE /\ connection' = 0
    /\ UNCHANGED <<generation,highwater,member,time,deadline,messages,network,receipts,projection,disclosed,fenced>>
VerifyPin(pin) ==
    /\ quarantine /\ pin = highwater
    /\ checkpoint' = TRUE
    /\ UNCHANGED <<generation,highwater,member,time,deadline,connection,messages,network,receipts,projection,disclosed,quarantine,fenced>>
Adopt == /\ quarantine /\ checkpoint /\ generation = highwater
         /\ quarantine' = FALSE
         /\ UNCHANGED <<generation,highwater,member,time,deadline,connection,messages,network,receipts,projection,disclosed,checkpoint,fenced>>
Fence == /\ ~fenced /\ fenced' = TRUE /\ connection' = 0
         /\ UNCHANGED <<generation,highwater,member,time,deadline,messages,network,receipts,projection,disclosed,quarantine,checkpoint>>
Next == Tick \/ Open \/ Partition \/ RestoreLatch \/ Adopt \/ Fence
        \/ (\E allow \in BOOLEAN : Policy(allow))
        \/ (\E id \in Ids : Request(id) \/ Project(id))
        \/ (\E msg \in (Ids \X (1..Generations)) : Receive(msg))
        \/ (\E pin \in 1..Generations : VerifyPin(pin))
Spec == Init /\ [][Next]_vars
TypeOK == /\ generation \in 1..Generations /\ highwater \in 1..Generations
          /\ time \in 0..3 /\ deadline \in 2..5 /\ receipts \subseteq Ids /\ projection \subseteq Ids
          /\ messages \subseteq (Ids \X (1..Generations))
CustodyBeforeProjection == projection \subseteq receipts
NoAuthorityRollback == generation = highwater
EpochFence == \A d \in disclosed : d[2] = d[3] /\ d[4] < d[5]
=============================================================================
