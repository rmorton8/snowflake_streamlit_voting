# Snowpark
import snowflake.connector
import streamlit as st
import pandas as pd
import plotly.express as px

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


covid_dict = {
    "test positive, but don't have COVID": 'test positive',
    "test negative, but do have COVID": 'test_negative'
}

if __name__ == "__main__":
    # Add header and a subheader
    st.title('Streamlit Demo -- this is a streamlit title function')
    st.subheader(
        "Powered by Snowpark for Python and Snowflake Data Marketplace | Made with Streamlit -- subheader function")
    st.header("Select which situations are less desirable header, then click the button to vote")


    # url = 'cOloRs aNd sIzE CaN bE aDjuStEd bY iNserTiNg HTML string iNtO a mArkDoWn fUncTiOn'
    # st.markdown(f'<p style="background-color:#0066cc;color:#33ff33;font-size:24px;border-radius:2%;">{url}</p>', unsafe_allow_html=True)

    def insert_row_into_snowflake(vote_choice):
        my_cnx = snowflake.connector.connect(**st.secrets['snowflake'])
        with my_cnx.cursor() as my_cur:
            my_cur.execute(f"insert into covid_votes values ('{vote_choice}')")
            return
        my_cnx.close()
        
    def grab_data_from_snowflake(table_name):
        my_cnx = snowflake.connector.connect(**st.secrets['snowflake'])
        with my_cnx.cursor() as my_cur:
            my_cur.execute(f"select * from {table_name}")
            return pd.DataFrame(my_cur.fetchall())
        my_cnx.close()
        
    # Use columns to display the three dataframes side-by-side along with their headers
    col1, col2 = st.columns(2)
    with st.container():
        with col1:
            st.subheader('Bob thinks he may have contracted COVID-19, and goes to get tested.')
            covid = st.selectbox("Which is less desirable?",
                                 tuple(covid_dict.keys()))
            if not st.button('Vote'):
                st.write('please vote')
            else:
                st.write(f'thanks for voting!')
                insert_row_into_snowflake(covid_dict[covid])

        with col2:
            covid_votes = grab_data_from_snowflake('COVID_VOTES')
#             st.write(type(covid_votes))
#             st.dataframe(covid_votes)
#             fig = px.pie(covid_votes, values=0, title='Votes')
            # Plot!
#             st.plotly_chart(fig, use_container_width=True)
            st.bar_chart(covid_votes)
#             st.subheader('ABC Bank monitors credit card usage to detect fraudulent activity.')
#             bank = st.selectbox("Which is less desirable?",
#                                 ("bank places a hold on your account, but there was no fraud",
#                                  "bank misses detecting fraud and no hold is placed"))



