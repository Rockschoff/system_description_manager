import streamlit as st
import snowflake.connector
from snowflake_data import SnowflakeData

if "snowflake_data" not in st.session_state:
    st.session_state.snowflake_data = SnowflakeData()

SNOWFLAKE_DATABASE = st.secrets["SNOWFLAKE"]["SNOWFLAKE_DATABASE"]
st.set_page_config(layout="wide")





st.header(SNOWFLAKE_DATABASE)
selected_system_name = st.selectbox("Select The System" , [system.system_name for system in st.session_state.snowflake_data.data])

selected_system = None
for system in st.session_state.snowflake_data.data:
    if system.system_name == selected_system_name:
        selected_system = system


st.write(f"Selected System Names are as follows : {selected_system_name}")

with st.expander("SQL_GENERATION_PROMPT"):
    new_sql_generation_prompt = st.text_area("SQL_GENERATION_PROMPT" , selected_system.sql_generation_prompt , label_visibility="hidden" )
    selected_system.update_sql_generation_prompt(new_sql_generation_prompt)

with st.expander("GRAPH_GENERATION_PROMPT"):
    new_graph_generation_prompt = st.text_area("GRAPH_GENERATION_PROMPT" , selected_system.graph_generation_prompt , label_visibility="hidden")
    selected_system.update_graph_generation_prompt(new_graph_generation_prompt)


with st.expander("Column Descriptions"):
    for index , column_description in enumerate(selected_system.column_descriptions):
        columns = st.columns([1 , 1 , 1, 5])
        columns[0].write(column_description.column_name)
        columns[1].write(column_description.datatype)
        with columns[2]:
            st.text_area("unique values" , column_description.unique_values , disabled=True , label_visibility="hidden", key=selected_system.system_name + column_description.column_name + "unique_values")
            include_unique_values = st.checkbox("Include unique values , if number of unique values in less than 200"  , key=selected_system.system_name + column_description.column_name + "checkbox" , value=False if column_description.unique_values == "" else True)
            if(include_unique_values and column_description.unique_values==""):
                column_description.load_unique_values()
                st.rerun()
            elif (not include_unique_values and column_description.unique_values != ""):
                column_description.remove_unique_values()
                st.rerun()


        with columns[3]:
            value = st.text_area("COLUMN DESCRIPTION" , column_description.description , key=selected_system.system_name + column_description.column_name)
            column_description.update_column_description_text(value)


