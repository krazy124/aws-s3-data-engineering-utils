import logging
import streamlit as st

logger = logging.getLogger(__name__)


def run_ui_safely(operation_name, func, *args, **kwargs):
    """
    Execute a function safely inside Streamlit.
    Shows user-friendly errors and logs details.
    """

    try:
        return func(*args, **kwargs)

    except Exception as e:
        logger.exception(f"{operation_name} failed")
        st.error(f"{operation_name} failed: {e}")
        return None
