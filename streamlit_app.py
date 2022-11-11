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
    "test negative, but do have COVID": 'test negative'
}

bank_dict = {
    "bank places hold on credit card, but no fraud occurred": 'credit hold',
    "bank doesn't place a hold, but there was fraud!": 'no-hold, but fraud!'
}

school_dict = {
    "Rejection letter, but it's a mistake and you were actually admitted!": 'false rejection',
    "Acceptance letter, but you were actually mean to be rejected!": 'false acceptance'
}


def insert_row_into_snowflake(vote_choice, table_name):
    my_cnx = snowflake.connector.connect(**st.secrets['snowflake'])
    with my_cnx.cursor() as my_cur:
        my_cur.execute(f"insert into {table_name} values ('{vote_choice}')")
    my_cnx.close()
    return


def grab_data_from_snowflake(table_name):
    my_cnx = snowflake.connector.connect(**st.secrets['snowflake'])
    with my_cnx.cursor() as my_cur:
        my_cur.execute(f"select * from {table_name}")
        output = pd.DataFrame(my_cur.fetchall())
    my_cnx.close()
    return output


def grab_and_plot_data(table_name, values):
    votes = grab_data_from_snowflake(table_name)
    if len(votes) >= 2:
        # transform votes
        counts = votes.value_counts()
        data_dict = {'choice': values, 'values': [counts[values[0]], counts[values[1]]]}
        final_df = pd.DataFrame(data_dict)
        # plot
        fig = px.pie(final_df, values='values', names='options', title='Voting Results')
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.write('waiting for votes')
    return


if __name__ == "__main__":
    # Add header and a subheader
    st.title('Streamlit Voting Demo')
    st.subheader(
        "Powered by Snowpark for Python and Snowflake Data Marketplace | Made with Streamlit")
    st.header("Vote for the situations you think are less desirable!")

    # COVID section
    col1, col2 = st.columns(2)
    with st.container():
        with col1:
            st.subheader('Bob thinks he may have contracted COVID-19, and goes to get tested.')
            output = st.selectbox("Which is less desirable?",
                                  tuple(covid_dict.keys()))
            if not st.button('Vote', key=1):
                st.write('please vote')
            else:
                st.write(f'thanks for voting!')
                table_name = "COVID_VOTES"
                vote_choice = covid_dict[output]
                my_cnx = snowflake.connector.connect(**st.secrets['snowflake'])
                with my_cnx.cursor() as my_cur:
                    my_cur.execute(f"insert into {table_name} values ('{vote_choice}')")
                my_cnx.close()

        with col2:
            my_cnx = snowflake.connector.connect(**st.secrets['snowflake'])
            with my_cnx.cursor() as my_cur:
                my_cur.execute(f"select * from {table_name}")
                votes = pd.DataFrame(my_cur.fetchall())
            my_cnx.close()
            if len(votes) >= 2:
                # transform votes
                counts = votes.value_counts()
                data_dict = {'choice': values, 'values': [counts[values[0]], counts[values[1]]]}
                final_df = pd.DataFrame(data_dict)
                # plot
                fig = px.pie(final_df, values='values', names='options', title='Voting Results')
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.write('waiting for votes')

    # Bank section
    col1, col2 = st.columns(2)
    with st.container():
        with col1:
            st.subheader('ABC Bank monitors credit card usage to detect any fraudulent activity.')
            output = st.selectbox("Which is less desirable?",
                                  tuple(bank_dict.keys()))
            if not st.button('Vote', key=2):
                st.write('please vote')
            else:
                st.write(f'thanks for voting!')
                insert_row_into_snowflake(bank_dict[output], 'BANK_VOTES')

        with col2:
            grab_and_plot_data('BANK_VOTES', values=list(bank_dict.values()))

        # SCHOOL section
    col1, col2 = st.columns(2)
    with st.container():
        with col1:
            st.subheader(
                "It's your senior year of highschool and you recieve an admissions letter from your dream school.")
            output = st.selectbox("Which is less desirable?",
                                  tuple(school_dict.keys()))
            if not st.button('Vote', key=3):
                st.write('please vote')
            else:
                st.write(f'thanks for voting!')
                insert_row_into_snowflake(school_dict[output], 'SCHOOL_VOTES')

        with col2:
            grab_and_plot_data('SCHOOL_VOTES', values=list(school_dict.values()))
