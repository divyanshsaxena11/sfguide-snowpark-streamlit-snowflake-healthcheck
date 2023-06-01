from snowflake.snowpark.session import Session
from snowflake.snowpark import functions as F
from snowflake.snowpark.types import *
import streamlit as st
import pandas as pd
import streamlit as st
from PIL import Image
import datetime
import plotly.express as px
from datetime import date, timedelta

# Streamlit config
st.set_page_config(
    page_title="Snowflake Health Check App",
    page_icon="❄️",
    # layout = 'wide',
    initial_sidebar_state="auto",
    menu_items={
        "Get Help": "https://developers.snowflake.com",
        "About": "The Application is built by Snowflake Data Superhero - [Divyansh Saxena](https://www.linkedin.com/in/divyanshsaxena/) . The source code for this application can be accessed on [GitHub](https://github.com/divyanshsaxena11/sfguide-snowpark-streamlit-snowflake-healthcheck) ",
    },
)


ss = st.session_state
session = ""
if not ss:
    ss.pressed_first_button = False


with st.sidebar:
    SF_ACCOUNT = st.text_input(
        "Enter Your Snowflake Account [<account_details>.snowflakecomputing.com] :"
    )
    SF_USR = st.text_input("Snowflake USER ( Divyansh ):")
    SF_PWD = st.text_input("Snowflake password:", type="password")
    conn = {"ACCOUNT": SF_ACCOUNT, "USER": SF_USR, "PASSWORD": SF_PWD}

    if st.button("Connect") or ss.pressed_first_button:
        session = Session.builder.configs(conn).create()
        ss.pressed_first_button = True
        st.success("Success!", icon="✅")

        if session != "":
            datawarehouse_list = session.sql("show warehouses;").collect()
            datawarehouse_list = pd.DataFrame(datawarehouse_list)
            datawarehouse_list = datawarehouse_list["name"]

            datawarehouse_option = st.selectbox(
                "Select Virtual warehouse", datawarehouse_list
            )
            set_warehouse = session.sql(
                f"""USE WAREHOUSE {datawarehouse_option}   ;"""
            ).collect()

with st.container():
    if session != "":
        st.title("Snowflake Health Check")
        image = Image.open(
            "banner.jpg",
        )
        st.image(
            image, caption="Community App Build By Data Superhero - Divyansh Saxena"
        )
        st.header(
            "Get better understanding of Snowflake's Resource Optimization and Performance capabilities on :red[Streamlit]"
        )

        date_range = st.date_input(
            "Select the Starting Date for Report Generation",
            (date.today() - timedelta(days=30)),
        )
        currentdate = datetime.datetime.today().strftime("%Y-%m-%d")
        if str(date_range) > currentdate:
            st.error("The date selected is greated than current date!", icon="🚨")

        tab1, tab2, tab3 = st.tabs(
            ["Warehouse Performance", "Users Profile", "Billing Metrics"]
        )

        # Elements for Warehouse Performances
        with tab1:
            sql_warehouse_performances = session.sql(
                """
             SELECT DATE_TRUNC('HOUR', START_TIME) AS QUERY_START_HOUR
               ,WAREHOUSE_NAME
                    ,COUNT(*) AS NUM_QUERIES
               FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
               WHERE START_TIME >= '"""
                + str(date_range)
                + """' AND WAREHOUSE_NAME IS NOT NULL
               GROUP BY 1, 2
               ORDER BY 1 DESC, 2
             """
            ).to_pandas()
            st.subheader(
                "Average number of queries run on an hourly basis - :red[Understand Query Activity]"
            )
            sql_warehouse_performances_pivot = sql_warehouse_performances.pivot_table(
                values="NUM_QUERIES", index="QUERY_START_HOUR", columns="WAREHOUSE_NAME"
            )
            st.area_chart(data=sql_warehouse_performances_pivot)

            st.subheader(
                "Queries by # of Times Executed and Execution Time - :red[Opportunity to materialize the result set]"
            )
            sq_exec_time_q_count = session.sql(
                """
             SELECT 
            QUERY_TEXT
            ,count(*) as number_of_queries
            ,sum(TOTAL_ELAPSED_TIME)/1000 as execution_seconds
            from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY Q
            where 1=1
            AND (QUERY_TEXT NOT ILIKE 'SHOW%' AND QUERY_TEXT NOT ILIKE 'USE%')
            and TO_DATE(Q.START_TIME) >     '"""
                + str(date_range)
                + """'
            and TOTAL_ELAPSED_TIME > 0 --only get queries that actually used compute
            group by 1
            having count(*) >= 10 --configurable/minimal threshold
            order by 2 desc
             """
            )
            st.dataframe(sq_exec_time_q_count)

            st.subheader(
                "Longest Running Queries - :red[opportunity to optimize with clustering or upsize the warehouse]"
            )
            sq_long_running_queries = session.sql(
                """
               select
                    
                    QUERY_ID
                    ,ROW_NUMBER() OVER(ORDER BY PARTITIONS_SCANNED DESC) as QUERY_ID_INT
                    ,QUERY_TEXT
                    ,TOTAL_ELAPSED_TIME/1000 AS QUERY_EXECUTION_TIME_SECONDS
                    ,PARTITIONS_SCANNED
                    ,PARTITIONS_TOTAL

            from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY Q
            where 1=1
            and TO_DATE(Q.START_TIME) >     '"""
                + str(date_range)
                + """'
                and TOTAL_ELAPSED_TIME > 0 --only get queries that actually used compute
                and ERROR_CODE iS NULL
                and PARTITIONS_SCANNED is not null
            
            order by  TOTAL_ELAPSED_TIME desc
            


             """
            ).to_pandas()
            st.dataframe(sq_long_running_queries)

            st.subheader(
                "Listing Warehouses with days where :red[Multi-Cluster Warehouse Could Have Helped]"
            )

            sql_wh_with_high_queue = session.sql(
                """
             SELECT TO_DATE(START_TIME) as DATE
            ,WAREHOUSE_NAME
            ,SUM(AVG_RUNNING) AS SUM_RUNNING
            ,SUM(AVG_QUEUED_LOAD) AS SUM_QUEUED
            FROM "SNOWFLAKE"."ACCOUNT_USAGE"."WAREHOUSE_LOAD_HISTORY"
            WHERE TO_DATE(START_TIME) >=  '"""
                + str(date_range)
                + """'
            GROUP BY 1,2
            HAVING SUM(AVG_QUEUED_LOAD) >0
             """
            ).to_pandas()
            sql_wh_with_high_queue_pivot = sql_wh_with_high_queue.pivot_table(
                values="SUM_RUNNING", index="DATE", columns="WAREHOUSE_NAME"
            )
            st.bar_chart(sql_wh_with_high_queue_pivot)

            sql_high_remote_spillage = session.sql(
                """
             SELECT QUERY_ID
            ,USER_NAME
            ,WAREHOUSE_NAME
            ,WAREHOUSE_SIZE
            ,BYTES_SCANNED
            ,BYTES_SPILLED_TO_REMOTE_STORAGE
            ,BYTES_SPILLED_TO_REMOTE_STORAGE / BYTES_SCANNED AS SPILLING_READ_RATIO
            FROM "SNOWFLAKE"."ACCOUNT_USAGE"."QUERY_HISTORY"
            WHERE BYTES_SPILLED_TO_REMOTE_STORAGE > BYTES_SCANNED * 5  -- Each byte read was spilled 5x on average
            and TO_DATE(START_TIME) >=  '"""
                + str(date_range)
                + """'
            ORDER BY SPILLING_READ_RATIO DESC


             """
            ).to_pandas()
            st.subheader(
                "Listing Queries where :red[Scaling Up Warehouse Could Have Helped]"
            )
            st.dataframe(sql_high_remote_spillage)

        with tab2:
            st.subheader(
                "Users queries that scan a lot of data - :red[opportunity to train the user or enable clustering]"
            )
            sql_users_heavy_scanners = session.sql(
                """
             select 
               User_name
               , warehouse_name
               , avg(case when partitions_total > 0 then partitions_scanned / partitions_total else 0 end) avg_pct_scanned
               from   snowflake.account_usage.query_history
               where  start_time::date > '"""
                + str(date_range)
                + """'
               and warehouse_name is not null
               group by 1, 2
               order by 3 desc
             """
            ).to_pandas()
            # st.dataframe(sql_users_heavy_scanners)
            sql_users_heavy_scanners_pivot = sql_users_heavy_scanners.pivot_table(
                values="AVG_PCT_SCANNED", index="WAREHOUSE_NAME", columns="USER_NAME"
            )
            st.bar_chart(sql_users_heavy_scanners_pivot)

            sql_users_with_full_q_scan = session.sql(
                """
             SELECT USER_NAME
               ,COUNT(*) as COUNT_OF_QUERIES
               FROM "SNOWFLAKE"."ACCOUNT_USAGE"."QUERY_HISTORY"
               WHERE START_TIME >= '"""
                + str(date_range)
                + """'
               AND PARTITIONS_SCANNED > (PARTITIONS_TOTAL*0.95)
               AND QUERY_TYPE NOT LIKE 'CREATE%'
               group by 1
               order by 2 desc
             
             """
            ).to_pandas()
            st.subheader(
                "Users with near Full Table Scans - :red[opportunity to train the user or enable clustering]"
            )
            st.dataframe(sql_users_with_full_q_scan, use_container_width=True)

            st.subheader(
                "Highly Active Users - :red[Identification of Expensive Users]"
            )
            sql_users_high_credits = session.sql(
                """
            select user_name, DATE(START_TIME) AS DATE, 
               round(sum(total_elapsed_time/1000 * 
               case warehouse_size 
               when 'X-Small' then 1/60/60 
               when 'Small'   then 2/60/60 
               when 'Medium'  then 4/60/60 
               when 'Large'   then 8/60/60 
               when 'X-Large' then 16/60/60 
               when '2X-Large' then 32/60/60 
               when '3X-Large' then 64/60/60 
               when '4X-Large' then 128/60/60 
               when '5X-Large' then 256/60/60 
               else 0 
               end),2) as estimated_credits 
               from snowflake.account_usage.query_history 
               WHERE START_TIME >= '"""
                + str(date_range)
                + """' group by 1,2 
               order by 3 desc 
                        """
            ).to_pandas()
            sql_users_high_credits_pivot = sql_users_high_credits.pivot_table(
                values="ESTIMATED_CREDITS", index="DATE", columns="USER_NAME"
            )
            st.area_chart(sql_users_high_credits_pivot)

        with tab3:
            # defining Metrix for Warehouse and Storage Usage

            # warehouse details
            col1, col2, col3 = st.columns(3)
            sql_credit_usage_wh = session.sql(
                """select ROUND(SUM(CREDITS_USED),2) AS CREDITS_USED
                , ROUND(SUM(CREDITS_USED_COMPUTE),2) AS CREDITS_USED_COMPUTE
                , ROUND(SUM(CREDITS_USED_CLOUD_SERVICES),2) AS CREDITS_USED_CLOUD_SERVICES
                from snowflake.account_usage.warehouse_metering_history
                WHERE START_TIME >= '"""
                + str(date_range)
                + """' 
                """
            ).to_pandas()

            with col1:
                st.metric(
                    label="Total Warehouse Credit Usage",
                    value=float(sql_credit_usage_wh["CREDITS_USED"]),
                )
            with col2:
                st.metric(
                    label="Total Compute Cost",
                    value=float(sql_credit_usage_wh["CREDITS_USED_COMPUTE"]),
                )
            with col3:
                st.metric(
                    label="Total Cloud Service Cost",
                    value=float(sql_credit_usage_wh["CREDITS_USED_CLOUD_SERVICES"]),
                )

            # storage usage details
            col4, col5, col6 = st.columns(3)
            sql_total_storage_size = session.sql(
                """
                  select ROUND(SUM(STORAGE_BYTES)/(1024*1024*1024),3) AS STORAGE_BYTES
                    , ROUND(SUM(FAILSAFE_BYTES)/(1024*1024*1024),3) AS FAILSAFE_BYTES
                    , ROUND(SUM(STAGE_BYTES)/(1024*1024*1024),3) AS STAGE_BYTES
                    from snowflake.account_usage.storage_usage
                    WHERE USAGE_DATE >= '"""
                + str(date_range)
                + """'                  
                  """
            ).to_pandas()
            with col4:
                st.metric(
                    label="Total Storage Usage in GBs",
                    value=float(sql_total_storage_size["STORAGE_BYTES"]),
                )
            with col5:
                st.metric(
                    label="Total Stage Usage in GBs",
                    value=float(sql_total_storage_size["STAGE_BYTES"]),
                )
            with col6:
                st.metric(
                    label="Total Failsafe Usage in GBs",
                    value=float(sql_total_storage_size["FAILSAFE_BYTES"]),
                )

            # SNOWFLAKE MATERIALIZED VIEW COST GRAPH

            st.subheader(
                "Materialized Views Cost History - :red[Quick identification of irregularities]"
            )

            sql_mv_cost_analysis = session.sql(
                """
             SELECT 
            TO_DATE(START_TIME) as DATE
            ,DATABASE_NAME||'.'||SCHEMA_NAME||'.'||TABLE_NAME as MV_NAME
            ,SUM(CREDITS_USED) as CREDITS_USED
            FROM "SNOWFLAKE"."ACCOUNT_USAGE"."MATERIALIZED_VIEW_REFRESH_HISTORY"
            WHERE START_TIME >= '"""
                + str(date_range)
                + """'
            GROUP BY 1,2
            ORDER BY 3 DESC 
             """
            ).to_pandas()
            sql_mv_cost_analysis_pivot = sql_mv_cost_analysis.pivot_table(
                values="CREDITS_USED", index="DATE", columns="MV_NAME"
            )

            st.area_chart(sql_mv_cost_analysis_pivot)

            # SNOWFLAKE SOS COST GRAPH

            st.subheader(
                "Search Optimization Cost History - :red[Quick identification of irregularities]"
            )
            sql_sos_cost = session.sql(
                """
             SELECT 
            TO_DATE(START_TIME) as DATE
            ,DATABASE_NAME||'.'||SCHEMA_NAME||'.'||TABLE_NAME as TABLE_NAME
            ,SUM(CREDITS_USED) as CREDITS_USED
            FROM "SNOWFLAKE"."ACCOUNT_USAGE"."SEARCH_OPTIMIZATION_HISTORY"

            WHERE START_TIME >= '"""
                + str(date_range)
                + """'
            GROUP BY 1,2
            ORDER BY 3 DESC 

             """
            ).to_pandas()
            sql_sos_cost_pivot = sql_sos_cost.pivot_table(
                values="CREDITS_USED", index="DATE", columns="TABLE_NAME"
            )
            st.bar_chart(sql_sos_cost_pivot)

            # SNOWFLAKE DATABASE REPLICATION COST GRAPH

            st.subheader(
                "Replication Cost History - :red[Quick identification of irregularities]"
            )
            sql_replication_cost = session.sql(
                """
             SELECT 
            TO_DATE(START_TIME) as DATE
            ,DATABASE_NAME
            ,SUM(CREDITS_USED) as CREDITS_USED
            FROM "SNOWFLAKE"."ACCOUNT_USAGE"."REPLICATION_USAGE_HISTORY"
            WHERE START_TIME >= '"""
                + str(date_range)
                + """' 
            GROUP BY 1,2
            ORDER BY 3 DESC 
             """
            ).to_pandas()
            st.dataframe(sql_replication_cost, use_container_width=True)

            st.subheader(
                "AutoClustering Cost History - :red[Quick identification of irregularities]"
            )

            # SNOWFLAKE AUTO CLUSTERING COST GRAPH
            sql_auto_clustering_cost = session.sql(
                """
             SELECT TO_DATE(START_TIME) as DATE
            ,DATABASE_NAME||'.'||SCHEMA_NAME||'.'||TABLE_NAME as TABLE_NAME
            ,SUM(CREDITS_USED) as CREDITS_USED
            FROM "SNOWFLAKE"."ACCOUNT_USAGE"."AUTOMATIC_CLUSTERING_HISTORY"
            WHERE START_TIME >= '"""
                + str(date_range)
                + """'  
            GROUP BY 1,2
            ORDER BY 3 DESC 

             """
            ).to_pandas()
            sql_auto_clustering_cost_pivot = sql_auto_clustering_cost.pivot_table(
                values="CREDITS_USED", index="DATE", columns="TABLE_NAME"
            )
            st.bar_chart(sql_auto_clustering_cost_pivot)

            # WAREHOUSE USAGE PIE CHART METRIX
            sql_warehouse_usage_matrix = session.sql(
                """
             select warehouse_name, sum(credits_used) as CREDITS_USED from snowflake.account_usage.warehouse_metering_history
               WHERE START_TIME >= '"""
                + str(date_range)
                + """'
               GROUP BY 1
             """
            ).to_pandas()

            st.subheader(
                "Warehouse Credit Consumption Chart - :red[Quick Analysis Of Expensive Warehouses]"
            )
            fig = px.pie(
                sql_warehouse_usage_matrix,
                values="CREDITS_USED",
                names="WAREHOUSE_NAME",
                color_discrete_sequence=px.colors.sequential.RdBu,
            )
            fig
