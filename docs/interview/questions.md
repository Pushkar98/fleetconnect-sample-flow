# Likely interview questions — Data & Integration Architect (CX)

## Architecture strategy & governance
1. You join on day one with no target architecture in place. What do you do in your first 90 days, and what does the first version of the roadmap look like?
2. How do you define architecture principles and standards that teams actually follow rather than ignore?
3. Walk us through a governance model: who approves designs, when an architecture review is mandatory, and how you avoid becoming a bottleneck.
4. How do you write and use ADRs? Give an example of a decision you recorded and later reversed.
5. How do you align the CX data architecture with enterprise architecture, security, data governance and AI strategy when those teams have conflicting priorities?

## Kafka & event-driven product data
6. When would you choose event streaming over APIs or batch for distributing product data, and when not?
7. How do you design Kafka topics: naming, partitioning, keys, retention, and compaction for product master data?
8. How do you guarantee ordering and avoid duplicates end to end? Explain idempotent producers, exactly-once, and idempotent consumers.
9. How do you handle poison messages and invalid data? Describe your dead-letter and replay strategy.
10. How do you size and make a Kafka platform resilient: replication factor, min.insync.replicas, multi-AZ or multi-region?
11. What does observability look like for an event-driven platform? Which metrics and alerts matter (e.g. consumer lag)?

## Avro, schema governance & data contracts
12. Why Avro over JSON or Protobuf, and what does a Schema Registry add?
13. Explain BACKWARD, FORWARD and FULL compatibility. Which would you standardise on and why?
14. A team needs to make a breaking change to a widely used schema. How do you handle it?
15. What is a data contract beyond the schema itself (ownership, SLAs, quality rules, semantics), and how do you enforce it in CI/CD?

## Consumer data, n8n & Snowflake
16. Design the customer data integration architecture from capture to activation (segmentation, personalisation).
17. Where does n8n fit compared with Kafka, an iPaaS or custom microservices? Where would you *not* use it?
18. How do you make n8n workflows production-grade: versioning, environments, secrets, error handling, monitoring?
19. Describe your Snowflake architecture: layers (raw/staging/curated), databases and schemas, roles, warehouses and cost control.
20. How would you get Kafka data into Snowflake (Snowpipe Streaming, the Kafka connector, batch), and what are the trade-offs?
21. How do you model customer data for segmentation and personalisation (identity resolution, golden record, SCDs)?

## Privacy, quality & security
22. How do you embed GDPR and consent requirements by design? Cover the right to be forgotten in Kafka and Snowflake.
23. How do you handle PII: masking, tokenisation, row access policies, encryption, and least privilege?
24. What data quality framework would you put in place, and where in the pipeline do checks run?

## API-led integration & cloud-native
25. Explain API-led connectivity (system / process / experience APIs) and how it coexists with event-driven design.
26. How do you approach CI/CD, infrastructure as code and environment promotion for data and integration assets?

## AI & agentic architecture
27. How would you design the architecture for AI agents and conversational interfaces that need customer and product data?
28. How do you govern what an AI agent is allowed to access or do? Cover tool boundaries, auditability and data leakage.

## Leadership & scenarios
29. Tell us about a time you had to convince engineering leaders to adopt a standard they resisted.
30. A marketing team wants real-time personalisation next quarter, but the data platform isn't ready. How do you respond and what do you propose?
31. Review this: a team proposes point-to-point integrations between five CX systems to hit a deadline. What do you say?
32. What modernisation would you prioritise in a CX ecosystem in the next 12 months, and how would you measure success?

## Geospatial (because the CX ecosystem includes a GIS team)
33. How would you standardise location data across teams (coordinate systems, geometry encoding, geocoding quality)?
34. What is H3, and why might you use it as a shared location key across Kafka, APIs and Snowflake?
