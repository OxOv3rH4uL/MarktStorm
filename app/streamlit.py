import streamlit as st

st.set_page_config(
    page_title="Marktstorm Analytics",
    layout="wide"
)

st.sidebar.title("Marktstorm")

page = st.sidebar.radio(
    "Navigation",
    [
        "Overview",
        "System Monitoring",
        "Experiments"
    ]
)

st.sidebar.divider()
st.sidebar.caption("Real-Time Retail Analytics")

if page == "Overview":
    st.title("Marktstorm Real-time orders")
    st.subheader("Order Analytics")

    grafana_url = (
        "http://localhost:3000/d/fg4gv0diznk00a/"
        "marktstorm-metrics?orgId=1&kiosk"
    )
    st.iframe(
        grafana_url,
        height=1000
    )

elif page == "System Monitoring":
    st.title("Marktstorm System Performance")
    st.subheader("Real-time performance analysis")

    grafana_url = (
        "http://localhost:3000/d/cg4eujds8x534a/"
        "marktstorm-system-metrics?orgId=1&kiosk"
    )
    st.iframe(
        grafana_url,
        height=1000
    )
