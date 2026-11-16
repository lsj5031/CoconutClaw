----------------------- MODULE WaitNoticeComposition -----------------------
EXTENDS Naturals, FiniteSets
CONSTANTS Limit, Unsafe
VARIABLES source, head, phase, cancelled, frozen, resumes, blocker, observed,
          notices, sending, sent, preference, native, generation
vars == <<source,head,phase,cancelled,frozen,resumes,blocker,observed,
          notices,sending,sent,preference,native,generation>>
Init == /\ source = "pending" /\ head = 1 /\ phase = "waiting" /\ cancelled = FALSE
        /\ frozen = 0 /\ resumes = 0 /\ blocker = 0 /\ observed = 0
        /\ notices = {} /\ sending = {} /\ sent = {} /\ preference = 0
        /\ native = 0 /\ generation = 0
Source(outcome) ==
    /\ source = "pending" /\ source' = outcome /\ blocker' = 1
    /\ UNCHANGED <<head,phase,cancelled,frozen,resumes,observed,notices,sending,sent,preference,native,generation>>
Resume ==
    /\ phase = "waiting" /\ ~cancelled /\ head = 1
    /\ (source = "completed" \/ (Unsafe /\ source = "unknown"))
    /\ head' = 2 /\ phase' = "queued" /\ resumes' = 1
    /\ frozen' = IF source = "completed" THEN 1 ELSE 2
    /\ UNCHANGED <<source,cancelled,blocker,observed,notices,sending,sent,preference,native,generation>>
Observe ==
    /\ phase = "waiting" /\ ~cancelled /\ blocker # observed
    /\ notices' = notices \cup {<<head,blocker>>} /\ observed' = blocker
    /\ UNCHANGED <<source,head,phase,cancelled,frozen,resumes,blocker,sending,sent,preference,native,generation>>
BeginNotice(n) ==
    /\ n \in notices /\ n[1] = head /\ n[2] = blocker
    /\ phase = "waiting" /\ ~cancelled /\ n \notin (sending \cup sent)
    /\ sending' = sending \cup {n}
    /\ UNCHANGED <<source,head,phase,cancelled,frozen,resumes,blocker,observed,notices,sent,preference,native,generation>>
Ack(n) == /\ n \in sending /\ sent' = sent \cup {n} /\ sending' = sending \ {n}
          /\ UNCHANGED <<source,head,phase,cancelled,frozen,resumes,blocker,observed,notices,preference,native,generation>>
Cancel == /\ ~cancelled /\ cancelled' = TRUE /\ phase' = "cancelled"
          /\ UNCHANGED <<source,head,frozen,resumes,blocker,observed,notices,sending,sent,preference,native,generation>>
Preference ==
    /\ preference < Limit /\ preference' = preference + 1
    /\ UNCHANGED <<source,head,phase,cancelled,frozen,resumes,blocker,observed,notices,sending,sent,native,generation>>
Start ==
    /\ phase = "queued" /\ ~cancelled
    /\ native' = preference /\ generation' = preference /\ phase' = "running"
    /\ UNCHANGED <<source,head,cancelled,frozen,resumes,blocker,observed,notices,sending,sent,preference>>
Next == Resume \/ Observe \/ Cancel \/ Preference \/ Start
        \/ (\E o \in {"completed","failed","unknown","cancelled"} : Source(o))
        \/ (\E n \in ((1..2) \X (0..1)) : BeginNotice(n) \/ Ack(n))
Spec == Init /\ [][Next]_vars
TypeOK == /\ source \in {"pending","completed","failed","unknown","cancelled"}
          /\ head \in 1..2 /\ resumes \in 0..1 /\ preference \in 0..Limit
          /\ notices \subseteq ((1..2) \X (0..1))
SuccessfulEvidenceOnly == resumes = 1 => frozen = 1
OneContinuation == head = 1 + resumes
NativePreferenceSnapshot == native = generation
NoticeOrigin == sent \subseteq notices
=============================================================================
