# ==================================================
# new_app.py
# Minimal Streamlit backbone for AWS S3 Utility App
# ==================================================

import streamlit as st

from src.aws_operations import *


# ====== s1.v1 - Page Setup ======

st.set_page_config(
    page_title="AWS S3 Utility",
    layout="wide"
)


# ====== s2.v1 - AWS Connection ======

s3_client = get_s3_client()

# s2b1.v1
if s3_client is None:
    st.stop()


# ====== s3.v1 - App Header ======

st.title("AWS S3 Utility")
st.caption("Clean rebuild version — focused, simple, and expandable.")


# ====== s4.v2 - Bucket Section ======

with st.container(border=True):

    st.subheader("Bucket")

    bucket_select_col, bucket_create_col, bucket_empty_col, bucket_delete_col = st.columns([
                                                                                           3, 1, 1, 1])

    with bucket_select_col:
        buckets = bucket_list()

        # s4b1.v1
        if buckets:
            selected_bucket = st.selectbox(
                "Select Bucket",
                buckets,
                index=None,
                placeholder="Select Bucket"
            )

        else:
            st.warning("No buckets found.")
            selected_bucket = None

    with bucket_create_col:
        st.write("")
        create_bucket_clicked = st.button(
            "Create New Bucket", use_container_width=True)

    with bucket_empty_col:
        st.write("")
        empty_bucket_clicked = st.button(
            "Empty Bucket",
            use_container_width=True,
            disabled=selected_bucket is None
        )

    with bucket_delete_col:
        st.write("")
        delete_bucket_clicked = st.button(
            "Delete Bucket",
            use_container_width=True,
            disabled=selected_bucket is None
        )

    # s4b2.v1
    if create_bucket_clicked:

        st.divider()
        st.subheader("Create New Bucket")

        new_bucket_name = st.text_input(
            "Bucket Name", placeholder="example: my-data-bucket")
        region = st.selectbox(
            "AWS Region", ["us-east-1", "us-east-2", "us-west-1", "us-west-2"])

        # s4b3.v1
        if st.button("Confirm Create Bucket"):

            # s4b4.v1
            if new_bucket_name:
                created = create_bucket(
                    bucket_name=new_bucket_name, region=region)

                # s4b5.v1
                if created:
                    st.success(
                        f'Bucket "{new_bucket_name}" created successfully.')
                    st.rerun()

                else:
                    st.error("Bucket was not created.")

            else:
                st.warning("Enter a bucket name first.")

    # s4b6.v1
    if empty_bucket_clicked:
        st.success(f'Bucket "{selected_bucket}" emptied.')

    # s4b7.v1
    if delete_bucket_clicked:
        st.success(f'Bucket "{selected_bucket}" deleted.')


# ====== s5.v2 - Folder / Root Section ======

# s5b1.v1
if selected_bucket:

    with st.container(border=True):

        st.subheader("Folder / Root")

        # s5b2.v1
        if "show_create_folder" not in st.session_state:
            st.session_state.show_create_folder = False

        # s5b3.v1
        if "selected_location" not in st.session_state:
            st.session_state.selected_location = None

        folder_select_col, folder_create_col, folder_upload_col, folder_download_col, folder_delete_col = st.columns([
                                                                                                                     3, 1, 1, 1, 1])

        with folder_select_col:

            folders = list_all_folders(selected_bucket)
            location_options = ["Bucket Root"] + folders

            selected_location = st.selectbox(
                "Select Folder / Root",
                location_options,
                index=None,
                placeholder="Select Folder / Root"
            )

            # s5b4.v1
            if selected_location:
                st.session_state.selected_location = selected_location

        folder_selected = st.session_state.selected_location is not None
        bucket_root_selected = st.session_state.selected_location == "Bucket Root"
        real_folder_selected = folder_selected and not bucket_root_selected

        with folder_create_col:

            st.write("")

            # s5b5.v1
            if st.button("Create New Folder", use_container_width=True):
                st.session_state.show_create_folder = True

        with folder_upload_col:

            st.write("")

            upload_here_clicked = st.button(
                "Upload Here",
                use_container_width=True,
                disabled=not folder_selected
            )

        with folder_download_col:

            st.write("")

            download_folder_clicked = st.button(
                "Download Folder",
                use_container_width=True,
                disabled=not real_folder_selected
            )

        with folder_delete_col:

            st.write("")

            delete_folder_clicked = st.button(
                "Delete Folder",
                use_container_width=True,
                disabled=not real_folder_selected
            )

        # s5b6.v1
        if st.session_state.show_create_folder:

            st.divider()
            st.subheader("Create New Folder")

            new_folder_name = st.text_input(
                "Folder Prefix Name",
                placeholder="example: raw/ or processed/"
            )

            # s5b7.v1
            if st.button("Confirm Create Folder"):

                # s5b8.v1
                if new_folder_name:

                    created_folder = create_folder(
                        selected_bucket,
                        new_folder_name
                    )

                    # s5b9.v1
                    if created_folder:

                        # s5b10.v1
                        if not new_folder_name.endswith("/"):
                            new_folder_name += "/"

                        st.session_state.selected_location = new_folder_name
                        st.session_state.show_create_folder = False

                        st.success(
                            f'Folder "{new_folder_name}" created successfully.'
                        )

                        st.rerun()

                    else:
                        st.error("Folder was not created.")

                else:
                    st.warning("Enter a folder name first.")

        # s5b11.v1
        if upload_here_clicked:
            st.success(
                f'Upload action selected for "{st.session_state.selected_location}".')

        # s5b12.v1
        if download_folder_clicked:
            st.success(
                f'Download folder action selected for "{st.session_state.selected_location}".')

        # s5b13.v1
        if delete_folder_clicked:
            st.success(
                f'Delete folder action selected for "{st.session_state.selected_location}".')
# ====== s6.v1 - File / Object Section ======

# s6b1.v1
if selected_bucket and st.session_state.selected_location:

    with st.container(border=True):

        st.subheader("File / Object")

        selected_location = st.session_state.selected_location
        all_files = list_files(selected_bucket)

        # s6b2.v2
        if selected_location == "Bucket Root":

            files = [
                file for file in all_files
                if "/" not in file
            ]

            display_to_full_path = {
                file: file for file in files
            }

        else:

            files = [
                file for file in all_files
                if file.startswith(selected_location)
                and file != selected_location
            ]

            display_to_full_path = {
                file.replace(selected_location, "", 1): file
                for file in files
            }

        file_select_col, file_download_col, file_delete_col = st.columns([
                                                                         4, 1, 1])

        with file_select_col:

            selected_display_files = st.multiselect(
                "Select File(s) / Object(s)",
                list(display_to_full_path.keys()),
                placeholder="Select File(s) / Object(s)"
            )

            selected_files = [
                display_to_full_path[file]
                for file in selected_display_files
            ]

            # s6b3.v1
            if selected_files:
                st.session_state.selected_files = selected_files
            else:
                st.session_state.selected_files = []

        files_selected = len(st.session_state.selected_files) > 0

        with file_download_col:
            st.write("")

            download_file_clicked = st.button(
                "Download File(s)",
                use_container_width=True,
                disabled=not files_selected
            )

        with file_delete_col:
            st.write("")

            delete_file_clicked = st.button(
                "Delete File(s)",
                use_container_width=True,
                disabled=not files_selected
            )

        # s6b5.v1
        if download_file_clicked:
            st.success(
                f'Download selected file(s): {st.session_state.selected_files}')

        # s6b6.v1
        if delete_file_clicked:
            st.success(
                f'Delete selected file(s): {st.session_state.selected_files}')
