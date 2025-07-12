import time
import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import altair as alt

# Database connection
DB_CONN_STRING = st.secrets["DB_CONN"]

def get_db_connection():
    """Get database connection"""
    return psycopg2.connect(DB_CONN_STRING)

def get_latest_depths_all():
    """Get latest depth data for all tokens"""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # Get the most recent entry for each token
        cur.execute("""
            SELECT DISTINCT ON (token) token, buy_depth, sell_depth, timestamp
            FROM depths
            WHERE buy_depth > 0 AND sell_depth > 0
            ORDER BY token, timestamp DESC
        """)
        results = cur.fetchall()
        return results
    except Exception as e:
        st.error(f"Error fetching latest depths: {e}")
        return []
    finally:
        cur.close()
        conn.close()

def get_historical_depths(token_symbol):
    """Get historical depth data for a specific token (last 7 days)"""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        seven_days_ago = datetime.now() - timedelta(days=7)
        cur.execute("""
            SELECT timestamp, buy_depth, sell_depth
            FROM depths
            WHERE token = %s
            AND timestamp >= %s
            AND buy_depth > 0
            AND sell_depth > 0
            ORDER BY timestamp ASC
        """, (token_symbol, seven_days_ago))
        results = cur.fetchall()
        
        if results:
            df = pd.DataFrame(results, columns=['Timestamp', 'Buy Depth (USD)', 'Sell Depth (USD)'])
            # Ensure proper decimal precision
            df['Buy Depth (USD)'] = pd.to_numeric(df['Buy Depth (USD)'], errors='coerce')
            df['Sell Depth (USD)'] = pd.to_numeric(df['Sell Depth (USD)'], errors='coerce')
            df.set_index('Timestamp', inplace=True)
            return df
        return None
    except Exception as e:
        st.error(f"Error fetching historical depths for {token_symbol}: {e}")
        return None
    finally:
        cur.close()
        conn.close()

def format_currency(value):
    """Format currency values in millions/thousands format like $1.11M"""
    if value >= 1_000_000:
        return f"${value/1_000_000:.2f}M"
    elif value >= 1_000:
        return f"${value/1_000:.1f}K"
    else:
        return f"${value:.0f}"

def get_last_update_time():
    """Get the timestamp of the most recent data update"""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT MAX(timestamp) FROM depths")
        result = cur.fetchone()
        return result[0] if result and result[0] else None
    except Exception as e:
        st.error(f"Error fetching last update time: {e}")
        return None
    finally:
        cur.close()
        conn.close()

def get_available_tokens():
    """Get list of tokens that have data in the database"""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT DISTINCT token FROM depths ORDER BY token")
        results = cur.fetchall()
        return [row[0] for row in results]
    except Exception as e:
        st.error(f"Error fetching available tokens: {e}")
        return []
    finally:
        cur.close()
        conn.close()

# Streamlit app configuration
st.set_page_config(
    page_title="Starknet Market Depth Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.title("Starknet ±2% Depth Dashboard via AVNU")
st.markdown("*All quotes are executed on AVNU against USDC. Data is updated every minute via AWS Lambda.*")

# Display last update time
last_update = get_last_update_time()
if last_update:
    time_diff = datetime.utcnow() - last_update
    if time_diff.total_seconds() < 300:  # Less than 5 minutes
        st.success(f"🟢 Last updated: {last_update.strftime('%Y-%m-%d %H:%M:%S')} ({int(time_diff.total_seconds())} seconds ago)")
    else:
        st.warning(f"🟡 Last updated: {last_update.strftime('%Y-%m-%d %H:%M:%S')} ({int(time_diff.total_seconds()/60)} minutes ago)")
else:
    st.error("❌ No data available")

# Get latest depth data
latest_depths = get_latest_depths_all()

if latest_depths:
    # Prepare data for display
    data = []
    for token, buy_depth, sell_depth, timestamp in latest_depths:
        data.append({
            'Token': token,
            'Buy Depth (USD)': format_currency(float(buy_depth)),
            'Sell Depth (USD)': format_currency(float(sell_depth)),
            'Last Updated': timestamp.strftime('%H:%M:%S')
        })
    
    # Display current depths table
    st.subheader("📊 Current Market Depths")
    df = pd.DataFrame(data)
    st.dataframe(df, use_container_width=True, hide_index=True)
    
    # Get available tokens for historical charts
    available_tokens = get_available_tokens()
    
    if available_tokens:
        st.subheader("📈 Historical Charts")
        
        # Create tabs for better organization
        if len(available_tokens) > 4:
            # Use selectbox for many tokens
            selected_token = st.selectbox("Select token to view history:", available_tokens)
            tokens_to_display = [selected_token] if selected_token else []
        else:
            # Show all tokens if there are few
            tokens_to_display = available_tokens
        
        # Display historical charts
        for token in tokens_to_display:
            hist_df = get_historical_depths(token)
            if hist_df is not None and not hist_df.empty:
                with st.container():
                    st.markdown(f"### {token} ±2% Depth History (Last 7 Days)")
                    
                    # Prepare data for altair chart
                    chart_data = hist_df.reset_index().melt(
                        id_vars=['Timestamp'],
                        value_vars=['Buy Depth (USD)', 'Sell Depth (USD)'],
                        var_name='Depth Type',
                        value_name='Value'
                    )
                    
                    # Create altair chart with proper formatting
                    chart = alt.Chart(chart_data).mark_line(strokeWidth=2).add_selection(
                        alt.selection_interval()
                    ).encode(
                        x=alt.X('Timestamp:T', title='Time'),
                        y=alt.Y('Value:Q',
                               title='Depth (USD)',
                               axis=alt.Axis(
                                   format='$.2s',
                                   labelExpr="datum.value >= 1000000 ? '$' + format(datum.value/1000000, '.1f') + 'M' : datum.value >= 1000 ? '$' + format(datum.value/1000, '.0f') + 'K' : '$' + format(datum.value, '.0f')"
                               )),
                        color=alt.Color('Depth Type:N',
                                       scale=alt.Scale(
                                           domain=['Buy Depth (USD)', 'Sell Depth (USD)'],
                                           range=['#00ff00', '#ff0000']
                                       ),
                                       legend=alt.Legend(title="Depth Type"))
                    ).properties(
                        height=400,
                        width='container'
                    ).resolve_scale(
                        y='independent'
                    )
                    
                    st.altair_chart(chart, use_container_width=True)
            else:
                st.info(f"No historical data available for {token}")
    
else:
    st.warning("No current depth data available. The Lambda function may not have run yet or there might be an issue with data collection.")

# Auto-refresh the page every 60 seconds
time.sleep(60)
st.rerun()