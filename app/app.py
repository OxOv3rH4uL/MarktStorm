import streamlit as st
import subprocess
import sys

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
        "Malformed Events",
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

elif page == "Malformed Events":
    st.title("Malformed Events")
    st.subheader("Analysis of Wrong Events")

    grafana_url = (
        "http://localhost:3000/d/ag4hvk9p034zke/"
        "malformed-events?orgId=1&kiosk"
    )
    st.iframe(
        grafana_url,
        height=1000
    )

elif page == "Experiments":
    st.title("Experiments")
    st.subheader("Kafka Events Generator")

    generators = {
        "Normal Production" : "generator/normal_producer.py",
        "Hot Key Flooding" : "generator/hotkey_producer.py",
        "Burst Traffic" : "generator/burst_traffic_producer.py",
        "Malformed Events" : "generator/malformed_events_producer.py"
    }
    process = st.session_state.get("process", None)

    if process is not None and process.poll() is None:
        st.success("Generator is running")
    else:
        st.info("No generator running")
        st.session_state["process"] = None

    for name, script in generators.items():

        if st.button(f"Start {name}", key=name):

            process = st.session_state["process"]

            if process is None or process.poll() is not None:
                st.session_state["process"] = subprocess.Popen(
                    [sys.executable , script]
                )
                st.success(f"{name} started!")
                st.rerun()
            else:
                st.warning("Another generator is already running.")

    if st.button("Stop Generator"):
        process = st.session_state["process"]

        if process is not None and process.poll() is None:
            process.terminate()
            process.wait()

        st.session_state["process"] = None
        st.warning("Generator stopped.")
        st.rerun()

