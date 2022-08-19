This project aims to demo the dbt slim CI functionality, as well as give a forum for discussing the --defer and --state flags in dbt CLI that make slim CI possible.

commands:
#### Setup
```
dbt clean
dbt deps
```
#### Production run and manifest.json creation
```
dbt run --profiles-dir .
#### Copy manifest.json from you production run
cp ./target/manifest.json ./dev-run-artifacts/manifest.json
```
#### Change a model
orders_by_customers.sql changes

#### Slim CI Run
```
#### State can also be an environment variable
dbt run --select state:modified --profiles-dir . --target ci --defer --state ./dev-run-artifacts
#### Drop temporary CI_ Schemas
dbt run-operation remove_mr_schemas --profiles-dir .
```

#### Slim CI
```
dbt run -m state:modified+1 1+exposure:*,state:modified+ --profiles-dir . --target ci --defer --state ./dev-run-artifacts
```
- Modified model and first order children
- Any exposure that had an upstream model changed


## Resources
- [Deferral](https://docs.getdbt.com/reference/node-selection/defer)
- [Understanding State](https://docs.getdbt.com/guides/legacy/understanding-state)
