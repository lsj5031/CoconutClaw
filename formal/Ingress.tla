------------------------------ MODULE Ingress ------------------------------
EXTENDS Naturals, FiniteSets
CONSTANTS N, Sessions, Unsafe
E == 1..N
VARIABLES phase, payload, frozen, cursor, inbox, control, outbox, calls
vars == <<phase, payload, frozen, cursor, inbox, control, outbox, calls>>
Scope(e) == IF Sessions = 2 /\ e = N THEN 2 ELSE 1
Init == /\ phase = [e \in E |-> "absent"]
        /\ payload = [e \in E |-> 0] /\ frozen = payload
        /\ calls = [e \in E |-> 0]
        /\ cursor = 0 /\ inbox = {} /\ control = {} /\ outbox = {}
Admit(e, bytes) ==
    /\ phase[e] = "absent"
    /\ phase' = [phase EXCEPT ![e] = "pending"]
    /\ payload' = [payload EXCEPT ![e] = bytes]
    /\ frozen' = [frozen EXCEPT ![e] = bytes]
    /\ cursor' = IF e > cursor THEN e ELSE cursor
    /\ UNCHANGED <<inbox, control, outbox, calls>>
Claim(e) ==
    /\ phase[e] = "pending"
    /\ \A older \in E : (older < e /\ Scope(older) = Scope(e)) =>
          (phase[older] \notin {"pending", "applying"} /\
           (phase[older] # "unknown" \/ e = N))
    /\ phase' = [phase EXCEPT ![e] = "applying"]
    /\ UNCHANGED <<payload, frozen, cursor, inbox, control, outbox, calls>>
Commit(e) ==
    /\ phase[e] = "applying" /\ e \notin inbox
    /\ phase' = [phase EXCEPT ![e] = "done"]
    /\ control' = control \cup {e} /\ outbox' = outbox \cup {e}
    /\ UNCHANGED <<payload, frozen, cursor, inbox, calls>>
TaskReceipt(e) ==
    /\ phase[e] = "applying" /\ e \notin inbox
    /\ inbox' = inbox \cup {e}
    /\ UNCHANGED <<phase,payload,frozen,cursor,control,outbox,calls>>
AmbiguousEffect(e) ==
    /\ phase[e] = "applying" /\ calls[e] < 2 /\ e \notin inbox
    /\ calls' = [calls EXCEPT ![e] = @ + 1]
    /\ phase' = [phase EXCEPT ![e] = "unknown"]
    /\ UNCHANGED <<payload, frozen, cursor, inbox, control, outbox>>
Recover(e) ==
    /\ phase[e] = "applying"
    /\ phase' = [phase EXCEPT ![e] = IF e \in inbox THEN "done" ELSE "unknown"]
    /\ UNCHANGED <<payload, frozen, cursor, inbox, control, outbox, calls>>
RetryUnknown(e) ==
    /\ Unsafe /\ phase[e] = "unknown"
    /\ phase' = [phase EXCEPT ![e] = "pending"]
    /\ UNCHANGED <<payload, frozen, cursor, inbox, control, outbox, calls>>
Next == \E e \in E :
          (\E bytes \in {1,2} : Admit(e, bytes)) \/ Claim(e) \/ Commit(e)
          \/ TaskReceipt(e) \/ AmbiguousEffect(e) \/ Recover(e) \/ RetryUnknown(e)
Spec == Init /\ [][Next]_vars
TypeOK == /\ phase \in [E -> {"absent","pending","applying","done","unknown"}]
          /\ calls \in [E -> 0..2] /\ cursor \in 0..N
          /\ inbox \subseteq E /\ control \subseteq E /\ outbox \subseteq E
FrozenIdentity == payload = frozen
CustodyCursor == \A e \in E : phase[e] # "absent" => e <= cursor
AtomicControl == control = outbox
NoAmbiguousRetry == \A e \in E : calls[e] <= 1
=============================================================================
