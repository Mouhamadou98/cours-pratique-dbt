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
- Check out JA DATATECH CONSULTING youtub channel


## WORKFLOWS

In the first step, we need to create a GitHub repository and install dbt on our local environment.

I created a virtual environment using Python 3:
python3 -m venv .venv

Then I installed dbt-duckdb (to connect dbt to DuckDB).
I’m working on macOS, and I also installed DuckDB via Homebrew.

### 1. Initialize the project

Run:
dbt init formation_dbt
This command automatically creates a new directory for your project.

### 2. Create the profiles.yml file

Inside the formation_dbt directory, create a profiles.yml file.
In this file, we’ll configure the connection parameters for our deployment target (like DuckDB).

In the dbt_project.yml file, there’s a key called profile with the value formation_dbt.
Copy this value and use it in your profiles.yml file.

### 3. Debug the connection

Run:
dbt debug
This command validates that your connection is properly configured.

### 4. Create the sources and transformations

Create a sources.yml file (to define your data sources) and a transformation file such as transform.sql.
In transform.sql, we’ll write our transformations (for example, using CTEs).

### 5. Tests

dbt natively includes testing capabilities to ensure our transformations are correct.
To set this up, create a packages.yml file and define the required packages (check the file for details).

Then run:
dbt deps
to install dependencies.

After that, create a schema.yml file to define your tests.
You can also write custom tests if needed.

### 6. Save transformations as a Parquet file

Check the top of the transform.sql file to see how to export the results as a Parquet file.

