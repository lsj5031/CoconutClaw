------------------------- MODULE RuntimeComposition -------------------------
EXTENDS Naturals, FiniteSets
CONSTANTS N, Cap, Unsafe, Live, Stall
J == 1..N
VARIABLES custody, inbox, occurrence, cursor, status, running, outcome, outbox,
          sending, receipt, sent, unknown, cancelled
vars == <<custody,inbox,occurrence,cursor,status,running,outcome,outbox,
          sending,receipt,sent,unknown,cancelled>>
Init == /\ custody = {} /\ inbox = {} /\ occurrence = {} /\ cursor = {}
        /\ status = [j \in J |-> "absent"] /\ running = {}
        /\ outcome = {} /\ outbox = {} /\ sending = {} /\ receipt = {} /\ sent = {}
        /\ unknown = {} /\ cancelled = {}
Accept(j) ==
    /\ status[j] = "absent"
    /\ custody' = custody \cup {j} /\ inbox' = inbox \cup {j}
    /\ occurrence' = occurrence \cup {j} /\ cursor' = cursor \cup {j}
    /\ status' = [status EXCEPT ![j] = "queued"]
    /\ UNCHANGED <<running,outcome,outbox,sending,receipt,sent,unknown,cancelled>>
Start(j) ==
    /\ status[j] = "queued" /\ j \in inbox /\ j \notin cancelled
    /\ Cardinality(running) < Cap
    /\ status' = [status EXCEPT ![j] = "running"] /\ running' = running \cup {j}
    /\ UNCHANGED <<custody,inbox,occurrence,cursor,outcome,outbox,sending,receipt,sent,unknown,cancelled>>
Finish(j) ==
    /\ status[j] = "running"
    /\ status' = [status EXCEPT ![j] = IF j \in cancelled THEN "unknown" ELSE "done"]
    /\ running' = running \ {j}
    /\ outcome' = IF Unsafe THEN outcome ELSE outcome \cup {j}
    /\ outbox' = outbox \cup {j}
    /\ unknown' = IF j \in cancelled THEN unknown \cup {j} ELSE unknown
    /\ UNCHANGED <<custody,inbox,occurrence,cursor,sending,receipt,sent,cancelled>>
Cancel(j) ==
    /\ ~Live /\ j \in inbox /\ j \notin cancelled
    /\ cancelled' = cancelled \cup {j}
    /\ status' = [status EXCEPT ![j] = IF @ = "queued" THEN "cancelled" ELSE @]
    /\ UNCHANGED <<custody,inbox,occurrence,cursor,running,outcome,outbox,sending,receipt,sent,unknown>>
Crash(j) ==
    /\ ~Live /\ status[j] = "running"
    /\ status' = [status EXCEPT ![j] = "unknown"] /\ running' = running \ {j}
    /\ unknown' = unknown \cup {j}
    /\ UNCHANGED <<custody,inbox,occurrence,cursor,outcome,outbox,sending,receipt,sent,cancelled>>
Send(j) ==
    /\ j \in outbox /\ j \notin (sending \cup sent)
    /\ sending' = sending \cup {j}
    /\ UNCHANGED <<custody,inbox,occurrence,cursor,status,running,outcome,outbox,receipt,sent,unknown,cancelled>>
Ack(j) ==
    /\ j \in sending /\ (~Stall \/ j # 1)
    /\ receipt' = receipt \cup {j} /\ sent' = sent \cup {j} /\ sending' = sending \ {j}
    /\ UNCHANGED <<custody,inbox,occurrence,cursor,status,running,outcome,outbox,unknown,cancelled>>
Next == \E j \in J : Accept(j) \/ Start(j) \/ Finish(j) \/ Cancel(j) \/ Crash(j) \/ Send(j) \/ Ack(j)
Fair == \A j \in J : WF_vars(Accept(j)) /\ WF_vars(Start(j)) /\ WF_vars(Finish(j))
                         /\ WF_vars(Send(j)) /\ WF_vars(Ack(j))
Spec == Init /\ [][Next]_vars
LiveSpec == Spec /\ Fair
TypeOK == /\ status \in [J -> {"absent","queued","running","done","unknown","cancelled"}]
          /\ running \subseteq J /\ sent \subseteq J /\ unknown \subseteq J
AtomicAdmission == custody = inbox /\ inbox = occurrence /\ occurrence = cursor
NoOrphanOutbox == outbox \subseteq outcome /\ outcome \subseteq inbox
Capacity == Cardinality(running) <= Cap
ReceiptBeforeSent == sent \subseteq receipt /\ receipt \subseteq outbox
AllSettled == <>(inbox = J /\ sent = J /\ \A j \in J : status[j] = "done")
=============================================================================
