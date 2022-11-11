# Snowpark
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import avg, sum, col,lit
import streamlit as st
import pandas as pd

st.set_page_config(
     page_title="Streamlit Demo",
     page_icon="🧊",
     layout="wide",
     initial_sidebar_state="expanded",
     menu_items={
         'Get Help': 'https://developers.snowflake.com',
         'About': "This is an *extremely* cool app powered by Snowpark for Python, Streamlit, and Snowflake Data Marketplace"
     }
)

# Create Session object
def create_session_object():
    connection_parameters = {
          "account": "ae08019.ca-central-1.aws",
          "user": "mortonworkshop",
          "password": "MtaXe0oO10eG",
          "role": "accountadmin",
          "warehouse": "compute_wh",
          "database": "environment_data_atlas",
          "schema": "environment"
       }
    session = Session.builder.configs(connection_parameters).create()
    print(session.sql('select current_warehouse(), current_database(), current_schema()').collect())
    return session

if __name__ == "__main__":
    session = create_session_object()

# Create Snowpark DataFrames that loads data from Knoema: Environmental Data Atlas
snow_df_co2 = session.table("ENVIRONMENT.EDGARED2019").filter(col('Indicator Name') == 'Fossil CO2 Emissions').filter(col('Type Name') == 'All Type')
snow_df_co2 = snow_df_co2.group_by('Location Name').agg(sum('$16').alias("Total CO2 Emissions")).filter(col('Location Name') != 'World').sort('Location Name')
    
# Forest Occupied Land Area by Country
snow_df_land = session.table("ENVIRONMENT.\"WBWDI2019Jan\"").filter(col('Series Name') == 'Forest area (% of land area)')
snow_df_land = snow_df_land.group_by('Country Name').agg(sum('$61').alias("Total Share of Forest Land")).sort('Country Name')
    
# Total Municipal Waste by Country
snow_df_waste = session.table("ENVIRONMENT.UNENVDB2018").filter(col('Variable Name') == 'Municipal waste collected')
snow_df_waste = snow_df_waste.group_by('Location Name').agg(sum('$12').alias("Total Municipal Waste")).sort('Location Name')
    
# Convert Snowpark DataFrames to Pandas DataFrames for Streamlit
pd_df_co2  = snow_df_co2.to_pandas()
pd_df_land = snow_df_land.to_pandas() 
pd_df_waste = snow_df_waste.to_pandas().iloc[:11]
    
# Add header and a subheader
st.title('Streamlit Demo -- this is a streamlit title function')
st.subheader("Powered by Snowpark for Python and Snowflake Data Marketplace | Made with Streamlit -- subheader function")
st.header("Select which situations are less desirable header, then click the button to vote")
    
# url = 'cOloRs aNd sIzE CaN bE aDjuStEd bY iNserTiNg HTML string iNtO a mArkDoWn fUncTiOn'
# st.markdown(f'<p style="background-color:#0066cc;color:#33ff33;font-size:24px;border-radius:2%;">{url}</p>', unsafe_allow_html=True)

def insert_row_into_snowflake(vote_choice):
    with my_cnx.cursor() as my_cur:
        my_cur.execute(f"insert into covid_votes values ('{vote_choice}')")    
# Use columns to display the three dataframes side-by-side along with their headers
col1, col2 = st.columns(2)
with st.container():
    with col1:
        st.subheader('Bob thinks he may have contracted COVID-19, and goes to get tested.')
        covid = st.selectbox("Which is less desirable?", ("test positive, but don't have COVID", "test negative, but do have COVID"))
        if not st.button('Vote'):
            st.write('please vote')
            
        else:
            st.write(f'thanks for voting!')
            
    with col2:
        st.subheader('ABC Bank monitors credit card usage to detect fraudulent activity.')
        covid = st.selectbox("Which is less desirable?", ("bank places a hold on your account, but there was no fraud",
                                                          "bank misses detecting fraud and no hold is placed"))
                        
        
    # Display an interactive chart to visualize CO2 Emissions by Top N Countries
with st.container():
    st.subheader('CO2 Emissions by Top N Countries')
    with st.expander(""):
        emissions_threshold = st.slider(label='Emissions Threshold',min_value=5000, value=20000, step=5000)
        pd_df_co2_top_n = snow_df_co2.filter(col('Total CO2 Emissions') > emissions_threshold).to_pandas()
        st.bar_chart(data=pd_df_co2_top_n.set_index('Location Name'), width=850, height=500, use_container_width=True)
#picture = st.camera_input('Take a picture')
#if picture:
#    st.image(picture)
