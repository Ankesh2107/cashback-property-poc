# 5-8 minute screen walk-through (suggested script)

Record your terminal plus a browser tab with `output/dashboard.html`. Run `bash demo.sh`; it pauses at each step.

| Time | Show | Say |
|---|---|---|
| 0:00-0:40 | README top + workflow diagram | "I built this as a coded Python and SQLite pipeline rather than n8n. SQLite gives transactions and unique keys, so reruns can't duplicate anything. Nothing is ever sent: there is no sending code, and the database rejects any draft that isn't NOT_SENT." |
| 0:40-1:40 | Demo step 1 and 2 | "90 raw rows, 84 unique leads. L002 merged into L001 after trimming and lowercasing the email. L005 is suppressed. L003 and L011 are purchased-list, so they're on hold. L007 has a malformed email, so it's invalid. The strictest status wins and the reasons are recorded." |
| 1:40-2:40 | Step 3 (draft queue) | "22 drafts are in the review queue as PENDING_REVIEW. Each has the right name and company, a 15-minute meeting ask and an opt-out placeholder. There are no invented claims, and nothing is sent." |
| 2:40-4:00 | Step 4 (events, suppression, tasks) | "Replies are classified by ordered rules. Interested replies create follow-up tasks, and questions create draft-response tasks, never sends. E002 unsubscribes L004. That goes on a persistent suppression list, the draft is cancelled, and the merged duplicate L049 is blocked too. E005 appears twice but there is one event and one task." |
| 4:00-4:50 | Step 5 (rerun) | "Rerunning changes nothing: 24 events, 9 tasks, L004 and L005 still suppressed." |
| 4:50-6:30 | Steps 6 and 7 (failures) | "Deliberate failure one: I crash the run after E012. 12 events are kept and the run is marked FAILED. Rerunning finishes with 24 events and 9 tasks, no duplicates. Failure two: the destination fails twice for E010. Retries are logged, then it succeeds. If it never recovers, the row goes to dead-letter and the rest of the batch carries on." |
| 6:30-7:30 | Step 8 and 9 + production slide | "Summary report and dashboard, then the 21 passing tests. For production: secrets management, API rate limits, the Australian Spam Act and Privacy Act, access controls, and keeping a human approval step." |

Tips: have the README "Assumptions" section ready, because they may ask you to change a rule live (for example, make `Broker` count as a mortgage broker: edit `SEGMENTS` / the role check in `build_leads`).
