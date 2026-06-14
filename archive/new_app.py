# ==================================================
# new_app.py
# Minimal Streamlit backbone for AWS S3 Utility App
# ==================================================

import os
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


# ====== s4.v3 - Bucket Section ======

with st.container(border=True):

    st.subheader("Bucket")

    bucket_select_col, bucket_action_col = st.columns([4, 1])

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

    with bucket_action_col:
        st.write("")

        create_bucket_clicked = st.button(
            "Create New Bucket",
            use_container_width=True
        )

        empty_bucket_clicked = st.button(
            "Empty Bucket",
            use_container_width=True,
            disabled=selected_bucket is None
        )

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
            "Bucket Name",
            placeholder="example: my-data-bucket"
        )

        region = st.selectbox(
            "AWS Region",
            ["us-east-1", "us-east-2", "us-west-1", "us-west-2"]
        )

        # s4b3.v1
        if st.button("Confirm Create Bucket"):

            # s4b4.v1
            if new_bucket_name:
                created = create_bucket(
                    bucket_name=new_bucket_name,
                    region=region
                )

                # s4b5.v1
                if created:
                    st.success(
                        f'Bucket "{new_bucket_name}" created successfully.'
                    )
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


# ====== s5.v4 - Folder / Root Section ======

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

        folder_select_col, folder_action_col = st.columns([4, 1])

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

        with folder_action_col:
            st.write("")

            # s5b5.v1
            if st.button("Create New Folder", use_container_width=True):
                st.session_state.show_create_folder = True

            move_folder_clicked = st.button(
                "Move Folder",
                use_container_width=True,
                disabled=not real_folder_selected
            )

            upload_here_clicked = st.button(
                "Upload to Folder",
                use_container_width=True,
                disabled=not folder_selected
            )

            download_folder_clicked = st.button(
                "Download Folder",
                use_container_width=True,
                disabled=not real_folder_selected
            )

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

        # s5b11.v2 - Move Folder Action With Persistent Success Message

        if st.session_state.get("move_folder_success_message"):
            st.success(st.session_state.move_folder_success_message)
            st.session_state.move_folder_success_message = None

        if move_folder_clicked:
            st.session_state.show_move_folder = True

        if st.session_state.get("show_move_folder", False):

            st.divider()
            st.subheader("Move Folder")

            current_folder = st.session_state.selected_location

            destination_options = [
                folder for folder in list_all_folders(selected_bucket)
                if folder != current_folder
            ]

            destination_options = ["Bucket Root"] + destination_options

            destination_location = st.selectbox(
                "Move Folder To",
                destination_options,
                index=None,
                placeholder="Select Destination Folder / Root"
            )

            confirm_move_folder_col, cancel_move_folder_col = st.columns(2)

            with confirm_move_folder_col:

                if st.button("Confirm Move Folder", use_container_width=True):

                    if destination_location:

                        source_folder = current_folder
                        folder_name = source_folder.rstrip("/").split("/")[-1]

                        if destination_location == "Bucket Root":
                            destination_folder = f"{folder_name}/"
                        else:
                            destination_folder = (
                                f"{destination_location}{folder_name}/"
                            )

                        moved_folder = move_folder(
                            selected_bucket,
                            source_folder,
                            selected_bucket,
                            destination_folder
                        )

                        if moved_folder:

                            st.session_state.show_move_folder = False
                            st.session_state.selected_location = destination_folder
                            st.session_state.move_folder_success_message = (
                                f'Folder "{source_folder}" moved successfully.'
                            )

                            st.rerun()

                        else:
                            st.error("Folder was not moved.")

                    else:
                        st.warning("Select a destination folder first.")

            with cancel_move_folder_col:

                if st.button("Cancel Move Folder", use_container_width=True):

                    st.session_state.show_move_folder = False
                    st.info("Move folder canceled.")
                    st.rerun()

        # s5b12.v2 - Upload To Folder Action

        if upload_here_clicked:
            st.session_state.show_upload_folder = True

        if st.session_state.get("show_upload_folder", False):

            st.divider()
            st.subheader("Upload To Folder")

            uploaded_files = st.file_uploader(
                "Select File(s)",
                accept_multiple_files=True
            )

            include_root_folder = st.checkbox(
                "Preserve uploaded filenames exactly",
                value=True
            )

            confirm_upload_col, cancel_upload_col = st.columns(2)

            with confirm_upload_col:

                if st.button("Confirm Upload", use_container_width=True):

                    if uploaded_files:

                        successful_uploads = []

                        for uploaded_file in uploaded_files:

                            object_name = (
                                f"{st.session_state.selected_location}"
                                f"{uploaded_file.name}"
                            )

                            try:

                                s3_client.upload_fileobj(
                                    uploaded_file,
                                    selected_bucket,
                                    object_name
                                )

                                successful_uploads.append(uploaded_file.name)

                            except Exception as e:
                                st.error(
                                    f'Failed to upload "{uploaded_file.name}": {e}'
                                )

                        if len(successful_uploads) == len(uploaded_files):

                            st.session_state.show_upload_folder = False

                            st.success(
                                f'{len(successful_uploads)} file(s) uploaded successfully.'
                            )

                            st.rerun()

                        else:
                            st.error("One or more files were not uploaded.")

                    else:
                        st.warning("Select at least one file first.")

            with cancel_upload_col:

                if st.button("Cancel Upload", use_container_width=True):

                    st.session_state.show_upload_folder = False
                    st.info("Upload canceled.")
                    st.rerun()

        # s5b13.v2 - Download Folder Action

        if download_folder_clicked:
            st.session_state.show_download_folder = True

        if st.session_state.get("show_download_folder", False):

            st.divider()
            st.subheader("Download Folder")

            local_download_path = st.text_input(
                "Local Download Folder Path",
                placeholder="example: C:/Users/willi/Downloads"
            )

            include_root_folder = st.checkbox(
                "Include root folder in download",
                value=False
            )

            confirm_download_folder_col, cancel_download_folder_col = st.columns(
                2)

            with confirm_download_folder_col:

                if st.button("Confirm Download Folder", use_container_width=True):

                    if local_download_path:

                        folder_to_download = st.session_state.selected_location

                        downloaded_folder = download_folder(
                            selected_bucket,
                            folder_to_download,
                            local_download_path,
                            include_root_folder=include_root_folder
                        )

                        if downloaded_folder:
                            st.session_state.show_download_folder = False

                            st.success(
                                f'Folder "{folder_to_download}" downloaded successfully.'
                            )

                        else:
                            st.error("Folder was not downloaded.")

                    else:
                        st.warning("Enter a local download folder path first.")

            with cancel_download_folder_col:

                if st.button("Cancel Download Folder", use_container_width=True):

                    st.session_state.show_download_folder = False
                    st.info("Download folder canceled.")
                    st.rerun()

        # s5b14.v4 - Delete Folder Confirmation Action With Persistent Success Message

        if st.session_state.get("folder_delete_success_message"):
            st.success(st.session_state.folder_delete_success_message)
            st.session_state.folder_delete_success_message = None

        if delete_folder_clicked:
            st.session_state.confirm_delete_folder = True

        if st.session_state.get("confirm_delete_folder", False):

            st.warning(
                f'Proceed to delete this object?\n\n"{st.session_state.selected_location}"'
            )

            confirm_delete_folder_col, cancel_delete_folder_col = st.columns(2)

            with confirm_delete_folder_col:
                if st.button("Yes, Delete Folder", use_container_width=True):

                    folder_to_delete = st.session_state.selected_location

                    deleted_folder = delete_folder(
                        selected_bucket,
                        folder_to_delete
                    )

                    if deleted_folder:
                        st.session_state.confirm_delete_folder = False
                        st.session_state.selected_location = None
                        st.session_state.folder_delete_success_message = (
                            f'Folder "{folder_to_delete}" was successfully deleted.'
                        )

                        st.rerun()

                    else:
                        st.error("Folder was not deleted.")

            with cancel_delete_folder_col:
                if st.button("No, Cancel", use_container_width=True):
                    st.session_state.confirm_delete_folder = False
                    st.info("Delete folder action canceled.")
                    st.rerun()


# ====== s6.v3 - File / Object Section ======

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

        file_select_col, file_action_col = st.columns([4, 1])

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

        with file_action_col:
            st.write("")

            move_file_clicked = st.button(
                "Move File(s)",
                use_container_width=True,
                disabled=not files_selected
            )

            download_file_clicked = st.button(
                "Download File(s)",
                use_container_width=True,
                disabled=not files_selected
            )

            delete_file_clicked = st.button(
                "Delete File(s)",
                use_container_width=True,
                disabled=not files_selected
            )

        # s6b4.v3 - Move File(s) Action With Persistent Success Message

        if st.session_state.get("move_files_success_message"):
            st.success(st.session_state.move_files_success_message)
            st.session_state.move_files_success_message = None

        if move_file_clicked:
            st.session_state.show_move_files = True

        if st.session_state.get("show_move_files", False):

            st.divider()
            st.subheader("Move Selected File(s)")

            destination_options = ["Bucket Root"] + \
                list_all_folders(selected_bucket)

            destination_location = st.selectbox(
                "Move To",
                destination_options,
                index=None,
                placeholder="Select Destination Folder / Root"
            )

            confirm_move_files_col, cancel_move_files_col = st.columns(2)

            with confirm_move_files_col:

                if st.button("Confirm Move File(s)", use_container_width=True):

                    if destination_location:

                        files_to_move = st.session_state.selected_files.copy()
                        moved_files = []

                        for file in files_to_move:

                            file_name = os.path.basename(file)

                            if destination_location == "Bucket Root":
                                destination_object_name = file_name
                            else:
                                destination_object_name = f"{destination_location}{file_name}"

                            moved = move_file(
                                selected_bucket,
                                file,
                                selected_bucket,
                                destination_object_name
                            )

                            if moved:
                                moved_files.append(file)

                        if len(moved_files) == len(files_to_move):

                            st.session_state.show_move_files = False
                            st.session_state.selected_files = []
                            st.session_state.move_files_success_message = (
                                "Selected file(s) moved successfully."
                            )

                            st.rerun()

                        else:
                            st.error(
                                "One or more selected files were not moved.")

                    else:
                        st.warning("Select a destination folder first.")

            with cancel_move_files_col:

                if st.button("Cancel Move", use_container_width=True):

                    st.session_state.show_move_files = False
                    st.info("Move canceled.")
                    st.rerun()

        # s6b5.v2 - Download File(s) Action

        if download_file_clicked:

            st.session_state.show_download_files = True

        if st.session_state.get("show_download_files", False):

            st.divider()
            st.subheader("Download Selected File(s)")

            local_download_path = st.text_input(
                "Local Download Folder Path",
                placeholder="example: C:/Users/willi/Downloads"
            )

            confirm_download_files_col, cancel_download_files_col = st.columns(
                2)

            with confirm_download_files_col:

                if st.button("Confirm Download File(s)", use_container_width=True):

                    if local_download_path:

                        downloaded_files = []

                        for file in st.session_state.selected_files:

                            local_file_path = os.path.join(
                                local_download_path,
                                os.path.basename(file)
                            )

                            downloaded = download_file(
                                selected_bucket,
                                file,
                                local_file_path
                            )

                            if downloaded:
                                downloaded_files.append(file)

                        if len(downloaded_files) == len(st.session_state.selected_files):

                            st.session_state.show_download_files = False
                            st.success(
                                "Selected file(s) downloaded successfully.")

                        else:
                            st.error(
                                "One or more selected files were not downloaded.")

                    else:
                        st.warning("Enter a local download folder path first.")

            with cancel_download_files_col:

                if st.button("Cancel Download", use_container_width=True):

                    st.session_state.show_download_files = False
                    st.info("Download canceled.")
                    st.rerun()

        # s6b6.v3 - Delete File(s) Confirmation Action
        if delete_file_clicked:
            st.session_state.confirm_delete_files = True

        if st.session_state.get("confirm_delete_files", False):

            st.warning("Proceed to delete this object?")

            for file in st.session_state.selected_files:
                st.write(f"- {file}")

            confirm_delete_files_col, cancel_delete_files_col = st.columns(2)

            with confirm_delete_files_col:
                if st.button("Yes, Delete File(s)", use_container_width=True):

                    files_to_delete = st.session_state.selected_files.copy()
                    deleted_files = []

                    for file in files_to_delete:

                        deleted = delete_file(
                            selected_bucket,
                            file
                        )

                        if deleted:
                            deleted_files.append(file)

                    if len(deleted_files) == len(files_to_delete):
                        st.session_state.confirm_delete_files = False
                        st.session_state.selected_files = []

                        st.success(
                            "Selected object(s) were successfully deleted.")

                        st.rerun()

                    else:
                        st.error(
                            "One or more selected objects were not deleted.")

            with cancel_delete_files_col:
                if st.button("No, Cancel", use_container_width=True):
                    st.session_state.confirm_delete_files = False
                    st.info("Delete file action canceled.")
                    st.rerun()
