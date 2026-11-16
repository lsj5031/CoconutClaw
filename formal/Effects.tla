------------------------------ MODULE Effects ------------------------------
EXTENDS Integers
CONSTANTS Retries, Unsafe
VARIABLES phase, network, attempts, external, receipt, revision, offer, audit
vars == <<phase, network, attempts, external, receipt, revision, offer, audit>>
Init == /\ phase = "ready" /\ network = "none"
        /\ attempts = 0 /\ external = 0 /\ receipt = FALSE
        /\ revision = 0 /\ offer = -1 /\ audit = FALSE
Begin == /\ phase = "ready" /\ attempts < Retries
         /\ phase' = "sending" /\ network' = "pending"
         /\ attempts' = attempts + 1
         /\ UNCHANGED <<external, receipt, revision, offer, audit>>
WireAccept == /\ phase = "sending" /\ network = "pending"
              /\ network' = "accepted" /\ external' = external + 1
              /\ UNCHANGED <<phase, attempts, receipt, revision, offer, audit>>
KnownReject == /\ phase = "sending" /\ network = "pending"
               /\ phase' = IF attempts < Retries THEN "ready" ELSE "suspended"
               /\ network' = "none"
               /\ UNCHANGED <<attempts, external, receipt, revision, offer, audit>>
Receipt == /\ phase = "sending" /\ network = "accepted" /\ ~receipt
           /\ receipt' = TRUE
           /\ UNCHANGED <<phase, network, attempts, external, revision, offer, audit>>
Project == /\ phase = "sending" /\ receipt
           /\ phase' = "sent"
           /\ UNCHANGED <<network, attempts, external, receipt, revision, offer, audit>>
Crash == /\ phase = "sending"
         /\ phase' = IF receipt THEN "sent" ELSE "unknown"
         /\ UNCHANGED <<network, attempts, external, receipt, revision, offer, audit>>
Offer == /\ phase = "sending" /\ receipt /\ offer = -1
         /\ offer' = revision
         /\ UNCHANGED <<phase, network, attempts, external, receipt, revision, audit>>
ChangeHead == /\ revision < 2 /\ phase # "sent"
              /\ revision' = revision + 1
              /\ UNCHANGED <<phase, network, attempts, external, receipt, offer, audit>>
Reconcile(actor) ==
    /\ phase = "sending" /\ receipt /\ offer = revision /\ actor = "owner"
    /\ phase' = "sent" /\ audit' = TRUE
    /\ UNCHANGED <<network, attempts, external, receipt, revision, offer>>
UnsafeRetry == /\ Unsafe /\ phase = "unknown" /\ attempts < Retries
               /\ phase' = "ready" /\ network' = "none"
               /\ UNCHANGED <<attempts, external, receipt, revision, offer, audit>>
Next == Begin \/ WireAccept \/ KnownReject \/ Receipt \/ Project \/ Crash
        \/ Offer \/ ChangeHead \/ (\E a \in {"owner","other"} : Reconcile(a)) \/ UnsafeRetry
Spec == Init /\ [][Next]_vars
TypeOK == /\ phase \in {"ready","sending","unknown","sent","suspended"}
          /\ network \in {"none","pending","accepted"}
          /\ attempts \in 0..Retries /\ external \in 0..Retries
          /\ receipt \in BOOLEAN /\ revision \in 0..2 /\ offer \in -1..2 /\ audit \in BOOLEAN
ReceiptAuthority == phase = "sent" => receipt
AuditEvidence == audit => receipt /\ external = 1
NoDuplicateExternal == external <= 1
=============================================================================
