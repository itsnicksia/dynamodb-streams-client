------------------------------ MODULE consumer ------------------------------
EXTENDS TLC, Integers, Sequences


(* --algorithm consumer

variables 
    time = 0;
    shard = [start |-> 0, end |-> 1000, enabled |-> TRUE, children |-> <<>>];
    iterators = <<>>;
    bookmarks = <<>>;
    processed = <<>>;

define
    TotallyOrdered ==
        \A i \in 0..Len(processed):
            (i + 1 > Len(processed)) \/ processed[i].sequenceNumber < processed[i+1].sequenceNumber
end define

macro IfIteratorExists(shardId, result) 
begin
    time := time + 1;
    result := FALSE;
end macro

macro RequestIterator(shardId) 
begin
    iterators := Append(iterators, [id |-> shardId]);
end macro

process consumer = 1
variables
    iteratorExists = FALSE;
    shardId = -1;
begin
    \* Make sure we have an iterator we can read 
    GetIterator:
        IfIteratorExists(shardId, iteratorExists);
        RequestIteratorExists:
            if ~iteratorExists then
                RequestIterator:
                    RequestIterator(shardId);
            end if;
        AssertIteratorExists:
            iteratorExists := TRUE
end process;
end algorithm; *)
\* BEGIN TRANSLATION (chksum(pcal) = "3678e8d5" /\ chksum(tla) = "a68dffef")
VARIABLES time, shard, iterators, bookmarks, processed, pc

(* define statement *)
TotallyOrdered ==
    \A i \in 0..Len(processed):
        (i + 1 > Len(processed)) \/ processed[i].sequenceNumber < processed[i+1].sequenceNumber

VARIABLES iteratorExists, shardId

vars == << time, shard, iterators, bookmarks, processed, pc, iteratorExists, 
           shardId >>

ProcSet == {1}

Init == (* Global variables *)
        /\ time = 0
        /\ shard = [start |-> 0, end |-> 1000, enabled |-> TRUE, children |-> <<>>]
        /\ iterators = <<>>
        /\ bookmarks = <<>>
        /\ processed = <<>>
        (* Process consumer *)
        /\ iteratorExists = FALSE
        /\ shardId = -1
        /\ pc = [self \in ProcSet |-> "GetIterator"]

GetIterator == /\ pc[1] = "GetIterator"
               /\ time' = time + 1
               /\ iteratorExists' = FALSE
               /\ pc' = [pc EXCEPT ![1] = "RequestIteratorExists"]
               /\ UNCHANGED << shard, iterators, bookmarks, processed, shardId >>

RequestIteratorExists == /\ pc[1] = "RequestIteratorExists"
                         /\ IF ~iteratorExists
                               THEN /\ pc' = [pc EXCEPT ![1] = "RequestIterator"]
                               ELSE /\ pc' = [pc EXCEPT ![1] = "AssertIteratorExists"]
                         /\ UNCHANGED << time, shard, iterators, bookmarks, 
                                         processed, iteratorExists, shardId >>

RequestIterator == /\ pc[1] = "RequestIterator"
                   /\ iterators' = Append(iterators, [id |-> shardId])
                   /\ pc' = [pc EXCEPT ![1] = "AssertIteratorExists"]
                   /\ UNCHANGED << time, shard, bookmarks, processed, 
                                   iteratorExists, shardId >>

AssertIteratorExists == /\ pc[1] = "AssertIteratorExists"
                        /\ iteratorExists' = TRUE
                        /\ pc' = [pc EXCEPT ![1] = "Done"]
                        /\ UNCHANGED << time, shard, iterators, bookmarks, 
                                        processed, shardId >>

consumer == GetIterator \/ RequestIteratorExists \/ RequestIterator
               \/ AssertIteratorExists

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == /\ \A self \in ProcSet: pc[self] = "Done"
               /\ UNCHANGED vars

Next == consumer
           \/ Terminating

Spec == Init /\ [][Next]_vars

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION 

=============================================================================
\* Modification History
\* Last modified Sat May 18 08:42:23 AWST 2024 by faste
\* Created Sat May 18 07:00:58 AWST 2024 by faste
