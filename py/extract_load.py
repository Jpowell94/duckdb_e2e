import duckdb
from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner
from prefect_dbt.cli import DbtCoreOperation
from soda.scan import Scan


@task
def extract_load():
    conn = duckdb.connect(
        "/home/jesserussellpowell/codeprojects/dataprojects/duckstuff/duckdb_e2e/duckdb_e2e/reports/covid_cases.db"
    )
    conn.sql(
        """
           INSTALL httpfs;
           LOAD httpfs;
           """
    )
    conn.sql(
        """CREATE TABLE us_counties As FROM read_csv_auto([
            'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties-2020.csv',
            'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties-2021.csv',
            'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties-2022.csv',
            'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties-2023.csv']);"""
    )
    conn.close()


@task
def validate():
    conn = duckdb.connect(
        "/home/jesserussellpowell/codeprojects/dataprojects/duckstuff/duckdb_e2e/duckdb_e2e/reports/covid_cases.db"
    )
    scan = Scan()
    scan.add_duckdb_connection(conn)
    scan.set_data_source_name("covid_data")
    scan.add_configuration_yaml_file(
        "/home/jesserussellpowell/codeprojects/dataprojects/duckstuff/duckdb_e2e/configuration.yaml"
    )
    scan.add_sodacl_yaml_files(
        "/home/jesserussellpowell/codeprojects/dataprojects/duckstuff/duckdb_e2e/checks.yaml"
    )
    scan.set_scan_definition_name("test_extracted_payload")
    scan.execute()
    scan.assert_no_checks_fail()
    print(scan.get_logs_text())
    conn.close()


@task
def transform():
    result = DbtCoreOperation(
        commands=["pwd", "dbt debug", "dbt run"],
        project_dir="/home/jesserussellpowell/codeprojects/dataprojects/duckstuff/duckdb_e2e/duckdb_e2e/",
        profiles_dir="/home/jesserussellpowell/codeprojects/dataprojects/duckstuff/duckdb_e2e/duckdb_e2e/",
    ).run()
    return result


@flow(task_runner=SequentialTaskRunner)
def elt():
    extract_load()
    transform()
    validate()


if __name__ == "__main__":
    elt()
