# Post-Call Processing Pipeline — Design Document

**Author:** \[Your Name\]  
**Date:** \[Date\]

* * *

## 1\. Assumptions

*State every assumption you made about the business, system, or environment. Be specific. These will be discussed in the follow-up.*

1.  ...
2.  ...

* * *

## 2\. Problem Diagnosis

*Before designing anything: what is actually broken, and why does it break at scale? In your own words.*

* * *

## 3\. Architecture Overview

*End-to-end flow from call-end webhook to completed analysis. Include a diagram.*

```
[Your architecture diagram — ASCII or Mermaid]
```

### Key design decisions

1.  ...
2.  ...

* * *

## 4\. Rate Limit Management

*This is the primary problem. How does your system respect LLM rate limits across 100K calls?*

### How you track rate limit usage

### How you decide what to process now vs. defer

### What happens when the limit is hit (recovery, not crash)

* * *

## 5\. Per-Customer Token Budgeting

*If total capacity is N tokens/min and K customers are active simultaneously:*

-   How do you allocate capacity across customers?
-   What guarantees does a customer with a pre-allocated budget receive?
-   What happens when a customer exceeds their budget?
-   What happens to unallocated headroom?

* * *

## 6\. Differentiated Processing

*Some call outcomes are time-sensitive. Some can wait. How do you determine which is which?*

*What mechanism do you use — is it a classification step, a flag set by the business, something else? Justify your choice.*

* * *

## 7\. Recording Pipeline

*Replacement for `asyncio.sleep(45s)`. How does it work? What does a failure look like to the on-call engineer?*

* * *

## 8\. Reliability & Durability

*How do you ensure no analysis result is permanently lost?*

* * *

## 9\. Auditability & Observability

*How would you debug a specific failed interaction 3 days after the fact?*

### What you log (and what fields every log event includes)

### Alert conditions

* * *

## 10\. Data Model

*Schema changes required. Show the SQL.*

```sql
-- Your schema additions/changes here
```

* * *

## 11\. Security

*What data in this system is sensitive? How do you protect it at rest and in transit?*

* * *

## 12\. API Interface

*Did you change the API contract (`POST /session/.../end`)? If yes, explain why. If no, explain why you kept it.*

* * *

## 13\. Trade-offs & Alternatives Considered

Option

Why Considered

Why Rejected / What You Chose Instead

...

...

...

* * *

## 14\. Known Weaknesses

*What are the gaps in your design? What would you address next?*

* * *

## 15\. What I Would Do With More Time

*Specific, prioritised list — not a generic wishlist.*

1.  ...
2.  ...