import streamlit as st
import snowflake.connector
from dataclasses import dataclass
from typing import TypedDict , List

SNOWFLAKE_ACCOUNT = st.secrets["SNOWFLAKE"]["SNOWFLAKE_ACCOUNT"]
SNOWFLAKE_USER = st.secrets["SNOWFLAKE"]["SNOWFLAKE_USER"]
SNOWFLAKE_PASSWORD = st.secrets["SNOWFLAKE"]["SNOWFLAKE_PASSWORD"]
SNOWFLAKE_DATABASE = st.secrets["SNOWFLAKE"]["SNOWFLAKE_DATABASE"]


def run_query_on_snowflake(query):
    """
    Executes a SQL query on Snowflake and returns the results.
    If an error occurs, it returns the error message.
    """

    try:
        # Establish a connection to Snowflake
        print("Making Connection")
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            database=SNOWFLAKE_DATABASE
        )

        # Create a cursor object to execute queries
        cursor = conn.cursor()

        # Execute the query
        print("running query")
        cursor.execute(query)

        # Fetch all results
        print("fetching results")
        results = cursor.fetchall()

        # Close the cursor and connection
        cursor.close()
        conn.close()

        return results  # Return query results

    except Exception as e:
        print(e)
        return []


@dataclass
class ColumnDescription():

    def __init__(self , system_name  , column_name , datatype , unique_values , description):
        self.system_name : str = system_name
        self.column_name : str = column_name
        self.datatype : str = datatype
        self.unique_values : str = unique_values
        self.description : str = description

    def load_unique_values(self):
        query = f"""
        SELECT DISTINCT {self.column_name} FROM {self.system_name}_SCHEMA.{self.system_name}_MAIN_TABLE LIMIT 200;
        """
        result = run_query_on_snowflake(query)

        if result:
            answer=""
            for row in result:
                answer += str(row[0])+",\n"
            self.unique_values=answer
            update_query = f"""
            UPDATE {self.system_name}_SCHEMA.{self.system_name}_MAIN_TABLE_DESCRIPTION
            SET UNIQUE_VALUES = '{answer}'
            WHERE COLUMN_NAME = '{self.column_name}'
            """
            update_result = run_query_on_snowflake(update_query)
            print("Loaded Unique Values")

        return

    def remove_unique_values(self):
        update_query = f"""
                    UPDATE {self.system_name}_SCHEMA.{self.system_name}_MAIN_TABLE_DESCRIPTION
                    SET UNIQUE_VALUES = ''
                    WHERE COLUMN_NAME = '{self.column_name}'
                    """
        update_result = run_query_on_snowflake(update_query)

        print("Removed Unique Values")
        self.unique_values=""
        return

    def update_column_description_text(self , new_value):
        if new_value==self.description:
            return

        update_query = f"""
                            UPDATE {self.system_name}_SCHEMA.{self.system_name}_MAIN_TABLE_DESCRIPTION
                            SET DESCRIPTION = '{new_value}'
                            WHERE COLUMN_NAME = '{self.column_name}'
                            """
        update_result = run_query_on_snowflake(update_query)
        print(f"column description for {self.column_name} , update successfully")
        return


@dataclass
class SystemData:

    def __init__(self , name):
        self.system_name = name
        self.column_descriptions : List[ColumnDescription] = []
        self.sql_generation_prompt=""
        self.graph_generation_prompt=""
        print(f"Loading System Name : {self.system_name}")
        self.load_generation_prompts()
        self.load_column_descriptions()

    def update_sql_generation_prompt(self , new_value : str):
        if new_value==self.sql_generation_prompt:
            return

        print(f"Updating to new value : {self.sql_generation_prompt[:10]}")
        query = f"""
        UPDATE {self.system_name}_SCHEMA.{self.system_name}_PROMPT_TABLE
        SET TEXT = '{new_value}'
        WHERE NAME = 'SQL_GENERATION_PROMPT'
        """
        self.sql_generation_prompt = new_value
        result = run_query_on_snowflake(query)
        print(result , "Update Complete")
        return

    def update_graph_generation_prompt(self , new_value):
        if new_value==self.graph_generation_prompt:
            return

        query = f"""
                UPDATE {self.system_name}_SCHEMA.{self.system_name}_PROMPT_TABLE
                SET TEXT = '{new_value}'
                WHERE NAME = 'GRAPH_GENERATION_PROMPT'
                """
        self.graph_generation_prompt = new_value
        result = run_query_on_snowflake(query)
        print(result)
        return

    def load_column_descriptions(self):

        query = f"""
        SELECT COLUMN_NAME , DATA_TYPE , UNIQUE_VALUES , DESCRIPTION FROM {self.system_name}_SCHEMA.{self.system_name}_MAIN_TABLE_DESCRIPTION
        """

        result = run_query_on_snowflake(query)

        if len(result)==0:
            return

        for row in result:
            self.column_descriptions.append(ColumnDescription(
                system_name=self.system_name,
                column_name=row[0],
                datatype=row[1],
                unique_values=row[2],
                description=row[3]
            ))

        return


    def load_generation_prompts(self):
        query = f"""
        SELECT TEXT FROM {self.system_name}_SCHEMA.{self.system_name}_PROMPT_TABLE WHERE NAME = 'SQL_GENERATION_PROMPT';
        """
        result = run_query_on_snowflake(query)
        if len(result)>0:
            self.sql_generation_prompt = result[0][0]
        else:
            self.sql_generation_prompt = ""

        query = f"""
                SELECT TEXT FROM {self.system_name}_SCHEMA.{self.system_name}_PROMPT_TABLE WHERE NAME = 'GRAPH_GENERATION_PROMPT';
                """
        result = run_query_on_snowflake(query)
        if len(result) > 0:
            self.graph_generation_prompt = result[0][0]
        else:
            self.graph_generation_prompt = ""


@dataclass
class SnowflakeData:



    def __init__(self):
        self.data : List[SystemData] = []
        self.load_snowflake_data()

    def load_snowflake_data(self):

        if True or "system_names" not in st.session_state:
            print("Fetching system names from Snowflake...")

            # Query to get all schema names, excluding INFORMATION_SCHEMA & PUBLIC
            query = """
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name NOT IN ('INFORMATION_SCHEMA', 'PUBLIC');
            """

            results = run_query_on_snowflake(query)

            # Extract system names and remove "_SCHEMA" suffix
            system_names = [row[0].replace("_SCHEMA", "") for row in results]

            # Store in session state
            st.session_state.system_names = system_names
            print("System names loaded:", system_names)

            for system_name in system_names:
                self.data.append(SystemData(system_name))




