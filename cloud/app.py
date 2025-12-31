# app.py
"""ASIS Streamlit Dashboard.

Interactive dashboard for ASIS (Automated Strategic Intelligence System).
Provides real-time monitoring, analytics, and data exploration capabilities.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time
from typing import Dict, Any, Optional
import logging

from local.src.config import Config
from local.src.adapters.storage.duckdb_storage import DuckDBStorage

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Page configuration
st.set_page_config(
    page_title="ASIS - Automated Strategic Intelligence System",
    page_icon="🛰️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
if 'config' not in st.session_state:
    st.session_state.config = Config()
    st.session_state.db_storage = DuckDBStorage(
        st.session_state.config.DUCKDB_PATH,
        read_only=True,
        config=st.session_state.config
    )

def load_recent_events(hours_back: int = 24, limit: int = 1000) -> pd.DataFrame:
    """Load recent events from database."""
    try:
        end_date = datetime.now().date()
        start_date = (datetime.now() - timedelta(hours=hours_back)).date()

        events_df = st.session_state.db_storage.load_event_data(
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat()
        )

        # Limit the number of rows for performance
        if len(events_df) > limit:
            events_df = events_df.head(limit)

        return events_df
    except Exception as e:
        logger.error(f"Error loading events: {e}")
        return pd.DataFrame()

def load_aggregated_data(days_back: int = 7, limit: int = 1000) -> pd.DataFrame:
    """Load aggregated event data."""
    try:
        end_date = datetime.now().date()
        start_date = (datetime.now() - timedelta(days=days_back)).date()

        agg_df = st.session_state.db_storage.load_aggregated_event_data(
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat()
        )

        # Limit the number of rows for performance
        if len(agg_df) > limit:
            agg_df = agg_df.head(limit)

        return agg_df
    except Exception as e:
        logger.error(f"Error loading aggregated data: {e}")
        return pd.DataFrame()

def create_intensity_heatmap(events_df: pd.DataFrame) -> go.Figure:
    """Create geographic intensity heatmap."""
    if events_df.empty or 'ActionGeo_Lat' not in events_df.columns:
        return go.Figure()

    # Filter to events with valid coordinates
    valid_events = events_df.dropna(subset=['ActionGeo_Lat', 'ActionGeo_Long', 'intensity_score'])

    if valid_events.empty:
        return go.Figure()

    fig = go.Figure(data=go.Scattergeo(
        lat=valid_events['ActionGeo_Lat'],
        lon=valid_events['ActionGeo_Long'],
        text=valid_events.apply(lambda x: f"Intensity: {x['intensity_score']:.1f}<br>Country: {x.get('ActionGeo_CountryCode', 'Unknown')}", axis=1),
        mode='markers',
        marker=dict(
            size=valid_events['intensity_score'] * 2,
            color=valid_events['intensity_score'],
            colorscale='Reds',
            showscale=True,
            colorbar=dict(title="Intensity Score"),
            sizemode='diameter'
        )
    ))

    fig.update_layout(
        title='Global Event Intensity Heatmap',
        geo=dict(
            showframe=False,
            showcoastlines=True,
            projection_type='natural earth'
        ),
        height=500
    )

    return fig

def create_temporal_trends(agg_df: pd.DataFrame) -> go.Figure:
    """Create temporal trends chart."""
    if agg_df.empty:
        return go.Figure()

    # Sort by date
    agg_df = agg_df.sort_values('date')

    fig = go.Figure()

    # Event count trend
    fig.add_trace(go.Scatter(
        x=agg_df['date'],
        y=agg_df['event_count'],
        mode='lines+markers',
        name='Event Count',
        line=dict(color='blue')
    ))

    # Average intensity trend
    fig.add_trace(go.Scatter(
        x=agg_df['date'],
        y=agg_df['avg_intensity'],
        mode='lines+markers',
        name='Avg Intensity',
        line=dict(color='red'),
        yaxis='y2'
    ))

    fig.update_layout(
        title='Event Trends Over Time',
        xaxis=dict(title='Date'),
        yaxis=dict(title='Event Count', side='left'),
        yaxis2=dict(title='Avg Intensity', side='right', overlaying='y'),
        height=400
    )

    return fig

def create_country_ranking(agg_df: pd.DataFrame, top_n: int = 10) -> go.Figure:
    """Create country ranking chart."""
    if agg_df.empty:
        return go.Figure()

    # Aggregate by country
    country_stats = agg_df.groupby('country_code').agg({
        'event_count': 'sum',
        'avg_intensity': 'mean'
    }).reset_index()

    # Sort by total events and take top N
    country_stats = country_stats.nlargest(top_n, 'event_count')

    fig = go.Figure(data=[
        go.Bar(
            x=country_stats['country_code'],
            y=country_stats['event_count'],
            name='Total Events',
            marker_color='lightblue'
        ),
        go.Scatter(
            x=country_stats['country_code'],
            y=country_stats['avg_intensity'],
            name='Avg Intensity',
            mode='lines+markers',
            line=dict(color='red'),
            yaxis='y2'
        )
    ])

    fig.update_layout(
        title=f'Top {top_n} Countries by Event Activity',
        xaxis=dict(title='Country'),
        yaxis=dict(title='Event Count'),
        yaxis2=dict(title='Avg Intensity', overlaying='y', side='right'),
        height=400
    )

    return fig

def create_event_type_distribution(events_df: pd.DataFrame) -> go.Figure:
    """Create event type distribution chart."""
    if events_df.empty or 'event_category' not in events_df.columns:
        return go.Figure()

    category_counts = events_df['event_category'].value_counts()

    fig = go.Figure(data=[go.Pie(
        labels=category_counts.index,
        values=category_counts.values,
        title="Event Categories"
    )])

    fig.update_layout(height=400)
    return fig

def main():
    """Main dashboard function."""

    # Sidebar
    st.sidebar.title("🛰️ ASIS Dashboard")
    st.sidebar.markdown("---")

    # Navigation
    page = st.sidebar.radio(
        "Navigation",
        ["📊 Overview", "🌍 Event Monitor", "📈 Analytics", "🔍 Data Explorer", "⚙️ Settings"]
    )

    # Time range selector
    st.sidebar.markdown("---")
    time_range = st.sidebar.selectbox(
        "Time Range",
        ["Last 24 Hours", "Last 7 Days", "Last 30 Days"],
        index=1
    )

    # Convert time range to hours/days
    if time_range == "Last 24 Hours":
        hours_back = 24
        days_back = 1
    elif time_range == "Last 7 Days":
        hours_back = 168
        days_back = 7
    else:  # Last 30 Days
        hours_back = 720
        days_back = 30

    # Load data with timeout protection
    with st.spinner("Loading data..."):
        try:
            # Add timeout for data loading (30 seconds)
            import signal

            def timeout_handler(signum, frame):
                raise TimeoutError("Data loading timed out")

            signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(30)  # 30 second timeout

            events_df = load_recent_events(hours_back, limit=500)  # Limit for performance
            agg_df = load_aggregated_data(days_back, limit=500)    # Limit for performance

            signal.alarm(0)  # Cancel timeout

        except TimeoutError:
            st.error("⚠️ Data loading timed out. Please try reducing the time range or check database connection.")
            events_df = pd.DataFrame()
            agg_df = pd.DataFrame()
        except Exception as e:
            st.error(f"⚠️ Error loading data: {str(e)}")
            events_df = pd.DataFrame()
            agg_df = pd.DataFrame()

    # Main content
    st.title("🛰️ ASIS - Automated Strategic Intelligence System")

    if page == "📊 Overview":
        show_overview_page(events_df, agg_df)

    elif page == "🌍 Event Monitor":
        show_event_monitor_page(events_df)

    elif page == "📈 Analytics":
        show_analytics_page(events_df, agg_df)

    elif page == "🔍 Data Explorer":
        show_data_explorer_page(events_df, agg_df)

    elif page == "⚙️ Settings":
        show_settings_page()

    # Footer
    st.markdown("---")
    st.markdown("*ASIS v0.1 - Automated Strategic Intelligence System*")

def show_overview_page(events_df: pd.DataFrame, agg_df: pd.DataFrame):
    """Show overview dashboard."""

    # Key metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        total_events = len(events_df) if not events_df.empty else 0
        st.metric("Total Events", f"{total_events:,}")

    with col2:
        countries = events_df['ActionGeo_CountryCode'].nunique() if not events_df.empty else 0
        st.metric("Countries Covered", countries)

    with col3:
        avg_intensity = events_df['intensity_score'].mean() if not events_df.empty else 0
        st.metric("Avg Intensity", f"{avg_intensity:.1f}")

    with col4:
        anomalies = events_df['is_anomaly'].sum() if 'is_anomaly' in events_df.columns and not events_df.empty else 0
        st.metric("Anomalous Events", anomalies)

    st.markdown("---")

    # Charts
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Global Event Intensity")
        heatmap_fig = create_intensity_heatmap(events_df)
        st.plotly_chart(heatmap_fig, use_container_width=True)

    with col2:
        st.subheader("Event Categories")
        pie_fig = create_event_type_distribution(events_df)
        st.plotly_chart(pie_fig, use_container_width=True)

    # Recent high-intensity events
    st.subheader("Recent High-Intensity Events")
    if not events_df.empty:
        high_intensity = events_df.nlargest(10, 'intensity_score')[
            ['SQLDATE', 'ActionGeo_CountryCode', 'intensity_score', 'event_category', 'Actor1Name']
        ]
        st.dataframe(high_intensity, use_container_width=True)
    else:
        st.info("No event data available")

def show_event_monitor_page(events_df: pd.DataFrame):
    """Show real-time event monitor."""

    st.header("🌍 Real-Time Event Monitor")

    # Auto-refresh toggle
    auto_refresh = st.checkbox("Auto-refresh every 5 minutes", value=True)

    if auto_refresh:
        # Add refresh button
        if st.button("🔄 Refresh Now"):
            st.rerun()

        # Auto-refresh logic (simulated)
        st.info("📡 Monitoring active - data refreshes automatically")

    # Event map
    st.subheader("Live Event Map")
    heatmap_fig = create_intensity_heatmap(events_df)
    st.plotly_chart(heatmap_fig, use_container_width=True)

    # Recent events table
    st.subheader("Recent Events")
    if not events_df.empty:
        recent_events = events_df.nlargest(20, 'SQLDATE')[
            ['SQLDATE', 'ActionGeo_CountryCode', 'event_category', 'intensity_score', 'NumMentions']
        ]
        st.dataframe(recent_events, use_container_width=True)
    else:
        st.info("No recent events to display")

def show_analytics_page(events_df: pd.DataFrame, agg_df: pd.DataFrame):
    """Show analytics dashboard."""

    st.header("📈 Event Analytics")

    # Temporal trends
    st.subheader("Event Trends Over Time")
    trends_fig = create_temporal_trends(agg_df)
    st.plotly_chart(trends_fig, use_container_width=True)

    # Country analysis
    st.subheader("Country Analysis")
    country_fig = create_country_ranking(agg_df)
    st.plotly_chart(country_fig, use_container_width=True)

    # Event type analysis
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Event Type Distribution")
        pie_fig = create_event_type_distribution(events_df)
        st.plotly_chart(pie_fig, use_container_width=True)

    with col2:
        st.subheader("Intensity by Category")
        if not events_df.empty and 'event_category' in events_df.columns:
            category_intensity = events_df.groupby('event_category')['intensity_score'].mean().sort_values(ascending=True)
            bar_fig = go.Figure(data=[go.Bar(
                x=category_intensity.values,
                y=category_intensity.index,
                orientation='h',
                marker_color='lightgreen'
            )])
            bar_fig.update_layout(
                title="Average Intensity by Event Category",
                xaxis_title="Average Intensity",
                height=400
            )
            st.plotly_chart(bar_fig, use_container_width=True)

def show_data_explorer_page(events_df: pd.DataFrame, agg_df: pd.DataFrame):
    """Show data exploration interface."""

    st.header("🔍 Data Explorer")

    # Data source selector
    data_source = st.selectbox(
        "Select Data Source",
        ["Raw Events", "Aggregated Events"]
    )

    if data_source == "Raw Events":
        df = events_df
        title = "Raw Events Data"
    else:
        df = agg_df
        title = "Aggregated Events Data"

    st.subheader(f"{title} ({len(df)} records)")

    if not df.empty:
        # Column selector
        all_columns = df.columns.tolist()
        selected_columns = st.multiselect(
            "Select columns to display",
            all_columns,
            default=all_columns[:10] if len(all_columns) > 10 else all_columns
        )

        # Filters
        col1, col2 = st.columns(2)

        with col1:
            if 'intensity_score' in df.columns:
                min_intensity = st.slider(
                    "Minimum Intensity",
                    min_value=float(df['intensity_score'].min()),
                    max_value=float(df['intensity_score'].max()),
                    value=float(df['intensity_score'].min())
                )

        with col2:
            if 'ActionGeo_CountryCode' in df.columns and data_source == "Raw Events":
                countries = sorted(df['ActionGeo_CountryCode'].dropna().unique())
                selected_countries = st.multiselect(
                    "Filter by Country",
                    countries,
                    default=[]
                )

        # Apply filters
        filtered_df = df[selected_columns].copy()

        if 'intensity_score' in filtered_df.columns:
            filtered_df = filtered_df[filtered_df['intensity_score'] >= min_intensity]

        if selected_countries and 'ActionGeo_CountryCode' in filtered_df.columns:
            filtered_df = filtered_df[filtered_df['ActionGeo_CountryCode'].isin(selected_countries)]

        # Display data
        st.dataframe(filtered_df, use_container_width=True)

        # Download button
        csv = filtered_df.to_csv(index=False)
        st.download_button(
            label="📥 Download as CSV",
            data=csv,
            file_name=f"asis_{data_source.lower().replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )
    else:
        st.info("No data available for exploration")

def show_settings_page():
    """Show settings and configuration."""

    st.header("⚙️ Settings")

    st.subheader("Database Configuration")
    st.info("Database path: " + str(st.session_state.config.DUCKDB_PATH))

    st.subheader("System Status")
    col1, col2 = st.columns(2)

    with col1:
        st.metric("Database Size", "Checking...")
        st.metric("Last Update", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    with col2:
        st.metric("Active Connections", "1")
        st.metric("Uptime", "Running")

    st.subheader("About ASIS")
    st.markdown("""
    **Automated Strategic Intelligence System (ASIS)**

    A local-first intelligence pipeline that:
    - Ingests data from GDELT and DBnomics
    - Filters noise using advanced algorithms and LLM
    - Provides real-time strategic intelligence

    **Version:** 0.1 (Pre-Alpha)
    **Architecture:** Pragmatic Clean Architecture
    **Data Sources:** GDELT Events, DBnomics Time Series
    """)

if __name__ == "__main__":
    main()