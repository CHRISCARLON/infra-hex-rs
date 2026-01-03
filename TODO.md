# TODO

V0.1.1

- Add in NUAR client
- Add in PipelineData trait

V0.1.2

- Add in UKPN client

V0.1.3

- The max page limit on opendatasoft APIs is 10,000
- Add in a in memory duckdb instance and use export endpoint for larger areas

## Reminder

To add a new infrastructure client (e.g., SGN), I need to:

 1. Create src/client/sgn/ with mod.rs, client.rs, record.rs
 2. Define SgnPipelineRecord and implement PipelineData for it
 3. Create SgnClient implementing InfraClient
 4. Add pub mod sgn; and re-exports to client/mod.rs
 5. Voilà!
