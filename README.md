This project aims to demo the dbt slim CI functionality, as well as give a forum for discussing the --defer and --state flags in dbt CLI that make slim CI possible.

commands:
#### Setup
dbt clean
dbt deps
#### Production run and manifest.json creation
dbt run --profiles-dir .
#### Copy manifest.json from you production run
cp ./target/manifest.json ./dev-run-artifacts/manifest.json

#### Change a model
orders_by_customers.sql changes

#### Slim CI Run
#### State can also be an environment variable
dbt run --select state:modified --profiles-dir . --target ci --defer --state ./dev-run-artifacts
#### Drop temporary CI_ Schemas
dbt run-operation remove_mr_schemas --profiles-dir .


Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices

