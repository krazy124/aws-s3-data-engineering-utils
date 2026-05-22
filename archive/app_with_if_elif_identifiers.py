# ==================================================
# IDENTIFIER SYSTEM
# ==================================================

# LEVEL 1
# | s | Section

# LEVEL 2
# | a | Subsection

# LEVEL 3
# | f | Function Call
# | b | Button
# | i | Input Element
# | l | List / Select Element
# | c | Container / Layout
# | x | Control Logic / Session State
# | m | Message / Status Output
# | d | Data Display / Render Output
# | e | If / Elif Branch
# | t | Text / Titles / Headers
# | u | Utility / Helper Logic

# LEVEL 4
# | v | Version

# EXAMPLE BREAKDOWN
# -----------------

# s4.v1 == Section 4 → Version 1

# s4`l1.v1 == Section 4 → List Element 1 → Version 1

# s4l1`c1.v1 == Section 4 → List Element 1 → Container 1 → Version 1

# s4l1c1`b1.v1 == Section 4 → List Element 1 → Container 1 → Button 1 → Version 1

# ==================================================

import streamlit as st
import base64

from src.aws_operations import *

# ====== s1.v1 - Streamlit Page Configuration Section ======
# s1`u1.v1
st.set_page_config(page_title="AWS S3 Utility Dashboard", layout="wide")


# ====== s1`a1.v1 - Background Image Styling Section ======
# s1a1`u1.v1
def get_base64_image(image_path):
    with open(image_path, "rb") as img_file:
        return base64.b64encode(img_file.read()).decode()


# s1a1`f1.v1
background_image = get_base64_image("assets/background.png")

# s1a1`d1.v1
st.markdown(
    f"""
    <style>

    .stApp {{
        background-image: url("data:image/png;base64,{background_image}");
        background-size: cover;
        background-position: center;
        background-attachment: fixed;
        background-repeat: no-repeat;
        background-color: rgba(0, 0, 0, 0.25);
        background-blend-mode: overlay;
    }}

    .main .block-container {{
        max-width: 1200px;
        padding-top: 2rem;
        padding-bottom: 2rem;
    }}

    div[data-testid="stVerticalBlock"] > div:has(div.element-container) {{
        background: rgba(8, 15, 30, 0.72);
        border-radius: 16px;
        padding: 1rem;
        backdrop-filter: blur(12px);
        border: 1px solid rgba(255, 255, 255, 0.08);
        box-shadow: 0 0 20px rgba(0, 0, 0, 0.25);
    }}

    .stButton > button {{
        background: rgba(15, 23, 42, 0.92);
        color: #f8fafc;
        border: 1px solid rgba(255, 255, 255, 0.18);
        border-radius: 8px;
        font-weight: 600;
        transition: all 0.2s ease;
    }}

    .stButton > button:hover {{
        border-color: #4da3ff;
        box-shadow: 0 0 12px rgba(77, 163, 255, 0.45);
        transform: translateY(-1px);
        color: #ffffff;
    }}

    .stButton > button:active {{
        transform: translateY(0px);
        border-color: #ff9900;
    }}

    h1, h2, h3, h4, h5, h6 {{
        color: #f8fafc !important;
        text-shadow: 0 2px 8px rgba(0, 0, 0, 0.45);
    }}

    p, label, span {{
        color: #e5ecff !important;
    }}

    div[data-baseweb="select"] > div {{
        background-color: rgba(15, 23, 42, 0.95);
        color: #f8fafc;
        border-radius: 8px;
        border: 1px solid rgba(255, 255, 255, 0.18);
    }}

    </style>
    """,
    unsafe_allow_html=True
)


# ====== s2.v1 - Streamlit Header Section ======
# s2`t1.v1
st.title("AWS S3 Utility Dashboard")

# s2`t2.v1
st.caption("Streamlit control panel for reusable AWS S3 utility functions.")


# ====== s3.v1 - Bucket Action Section ======
# s3`t1.v1
st.header("Bucket Setup")

# s3`c1.v1
col1, col2 = st.columns(2)

# s3c1`b1.v1
with col1:
    select_bucket_clicked = st.button(
        "Select Existing Bucket", use_container_width=True)

# s3c1`b2.v1
with col2:
    create_bucket_clicked = st.button(
        "Create New Bucket", use_container_width=True)


# ====== s3`x1.v1 - Bucket Mode Session State Control ======
# s3x1`e1.v1
if "bucket_mode" not in st.session_state:
    st.session_state.bucket_mode = None

# s3x1`e2.v1
if select_bucket_clicked:
    st.session_state.bucket_mode = "select"

# s3x1`e3.v1
if create_bucket_clicked:
    st.session_state.bucket_mode = "create"


# ====== s4.v3 - Existing Bucket Workflow Section ======
# s4`e1.v1
if st.session_state.bucket_mode == "select":
    # s4`t1.v1
    st.subheader("Select Existing Bucket")

    # s4`f1.v1
    buckets = bucket_list()

    # s4e1`e1.v1
    if buckets:

        # s4`l1.v1
        selected_bucket = st.selectbox("Choose a Bucket", buckets)

        # s4l1`t1.v1
        st.subheader("Bucket Actions")

        # s4l1`c1.v2
        col3, col4, col5 = st.columns(3)

        # s4l1c1`b1.v3
        with col3:
            list_files_clicked = st.button(
                "List Objects", use_container_width=True)

        # s4l1c1`b2.v2
        with col4:
            list_prefixes_clicked = st.button(
                "List Folders (Prefixes)", use_container_width=True)

        # s4l1c1`b3.v2
        with col5:
            create_folder_clicked = st.button(
                "Create Folder (Prefix)", use_container_width=True)

        # s4l1`c2.v2
        col6, col7 = st.columns(2)

        # s4l1c2`b1.v2
        with col6:
            upload_folder_clicked = st.button(
                "Upload Folder", use_container_width=True)

        # s4l1c2`b2.v2
        with col7:
            download_folder_clicked = st.button(
                "Download Folder", use_container_width=True)

        # ====== s4`a1.v1 - Bucket Danger Zone Section ======
        # s4a1`t1.v1
        st.subheader("Danger Zone")

        # s4a1`c1.v2
        col8, col9 = st.columns(2)

        # s4a1c1`b1.v2
        with col8:
            empty_bucket_clicked = st.button(
                "Empty Bucket", use_container_width=True)

        # s4a1c1`b2.v2
        with col9:
            delete_bucket_clicked = st.button(
                "Delete Bucket", use_container_width=True)

        # ====== s4`x1.v2 - Bucket Action Mode Control ======
        # s4x1`e1.v1
        if list_files_clicked:
            st.session_state.bucket_action = "list_files"

        # s4x1`e2.v1
        if list_prefixes_clicked:
            st.session_state.bucket_action = "list_prefixes"

        # s4x1`e3.v1
        if create_folder_clicked:
            st.session_state.bucket_action = "create_folder"

        # s4x1`e4.v1
        if upload_folder_clicked:
            st.session_state.bucket_action = "upload_folder"

        # s4x1`e5.v1
        if download_folder_clicked:
            st.session_state.bucket_action = "download_folder"

        # s4x1`e6.v1
        if empty_bucket_clicked:
            st.session_state.bucket_action = "empty_bucket"

        # s4x1`e7.v1
        if delete_bucket_clicked:
            st.session_state.bucket_action = "delete_bucket"

        # ====== s4`a2.v2 - Bucket Action Output Section ======
        # s4a2`x1.v1
        if "bucket_action" in st.session_state:

            # s4a2`e1.v1
            if st.session_state.bucket_action == "list_files":

                # s4a2`f1.v2
                files = list_files(selected_bucket)

                # s4a2e1`e1.v1
                if files:

                    # s4a2f1`m1.v2
                    st.success(f"Found {len(files)} file(s).")

                    # s4a2f1`d1.v2
                    for file in files:
                        st.write(file)

                else:

                    # s4a2f1`m2.v2
                    st.warning("No files found in this bucket.")

            # s4a2`e2.v1
            elif st.session_state.bucket_action == "list_prefixes":

                # s4a2`f2.v1
                folders = list_folders(selected_bucket)

                # s4a2e2`e1.v1
                if folders:

                    # s4a2f2`m1.v1
                    st.success(f"Found {len(folders)} folder prefix(es).")

                    # s4a2f2`d1.v1
                    for folder in folders:
                        st.write(folder)

                else:

                    # s4a2f2`m2.v1
                    st.warning("No folder prefixes found in this bucket.")

            # s4a2`e3.v1
            elif st.session_state.bucket_action == "create_folder":

                # s4a2`i1.v1
                new_folder_name = st.text_input(
                    "Folder Prefix Name",
                    placeholder="example: raw/ or processed/"
                )

                # s4a2i1`b1.v1
                # s4a2e3`e1.v1
                if st.button("Create Folder Prefix", use_container_width=True):
                    if new_folder_name:

                        # s4a2i1b1`f1.v1
                        created_folder = create_folder(
                            selected_bucket,
                            new_folder_name
                        )

                        if created_folder:
                            # s4a2i1b1f1`m1.v1
                            st.success(
                                f'Folder prefix "{new_folder_name}" created successfully.'
                            )
                        else:
                            # s4a2i1b1f1`m2.v1
                            st.error(
                                f'Folder prefix "{new_folder_name}" was not created.'
                            )
                    else:
                        # s4a2i1b1`m1.v1
                        st.warning("Enter a folder prefix name first.")

            # s4a2e4.v2
            elif st.session_state.bucket_action == "upload_folder":

                # s4a2`t2.v3
                st.subheader("Upload Local Folder")

                # s4a2`i2.v1
                local_upload_path = st.text_input(
                    "Local Folder Path",
                    placeholder=r"example: C:\Users\willi\Desktop\s3_upload_test"
                )

                # s4a2`f4.v1
                available_upload_folders = list_folders(selected_bucket)

                # s4a2`l3.v1
                if available_upload_folders:
                    s3_upload_prefix = st.selectbox(
                        "Choose S3 Folder",
                        available_upload_folders
                    )
                else:
                    s3_upload_prefix = st.text_input(
                        "S3 Folder",
                        placeholder="example: raw/ or uploads/test/"
                    )

                # s4a2`i5.v2
                upload_mode = st.radio(
                    "Upload Mode",
                    [
                        "Upload the folder and its files",
                        "Upload only the files"
                    ]
                )

                # s4a2`b2.v3
                if st.button("Confirm Upload Folder", use_container_width=True):

                    # s4a2e4b2`e1.v1
                    if not local_upload_path:
                        st.warning("Enter a local folder path first.")

                    # s4a2e4b2`e2.v1
                    elif not s3_upload_prefix:
                        st.warning("Choose or enter an S3 folder first.")

                    # s4a2e4b2`e3.v1
                    else:

                        # s4a2b2`x1.v1
                        include_root_folder = (
                            upload_mode == "Upload the folder and its files"
                        )

                        # s4a2b2`f1.v3
                        uploaded = upload_folder(
                            selected_bucket,
                            s3_upload_prefix,
                            local_upload_path,
                            include_root_folder=include_root_folder
                        )

                        # s4a2e4b2e3`e1.v1
                        if uploaded:

                            # s4a2b2f1`m1.v3
                            st.success(
                                f'Uploaded "{local_upload_path}" '
                                f'to "{selected_bucket}/{s3_upload_prefix}".'
                            )

                        # s4a2e4b2e3`e2.v1
                        else:

                            # s4a2b2f1`m2.v3
                            st.error(
                                "Folder was not uploaded. "
                                "Check the local path, AWS permissions, or logs."
                            )

            # s4a2`e5.v1
            elif st.session_state.bucket_action == "download_folder":

                # s4a2`t3.v1
                st.subheader("Download S3 Prefix to Local Folder")

                # s4a2`f3.v1
                available_folders = list_folders(selected_bucket)

                # s4a2`l2.v1
                # s4a2e5`e1.v1
                if available_folders:
                    s3_download_prefix = st.selectbox(
                        "Choose S3 Folder Prefix",
                        available_folders
                    )
                else:
                    s3_download_prefix = st.text_input(
                        "S3 Folder Prefix",
                        placeholder="example: raw/ or processed/"
                    )

                # s4a2`i4.v1
                local_download_path = st.text_input(
                    "Local Download Folder Path",
                    placeholder=r"example: C:\Users\willi\Desktop\s3_download_test"
                )

                # s4a2`m4.v2
                st.info(
                    "This will download every object under the selected S3 prefix into the local folder path you enter."
                )

                # s4a2`b3.v1
                # s4a2e5`e2.v1
                if st.button("Confirm Download Folder", use_container_width=True):
                    if not s3_download_prefix:
                        st.warning(
                            "Enter or choose an S3 folder prefix first.")

                    elif not local_download_path:
                        st.warning("Enter a local download folder path first.")

                    else:
                        downloaded = download_folder(
                            selected_bucket,
                            s3_download_prefix,
                            local_download_path
                        )

                        if downloaded:
                            st.success(
                                f'Downloaded "{selected_bucket}/{s3_download_prefix}" to "{local_download_path}".'
                            )
                        else:
                            st.error(
                                "Folder was not downloaded. Check the S3 prefix, AWS permissions, or logs."
                            )

            # s4a2`e6.v1
            elif st.session_state.bucket_action == "empty_bucket":

                # s4a2`m5.v2
                st.warning(
                    f'You are about to empty bucket "{selected_bucket}".'
                )

                # s4a2m5`i1.v1
                confirm_empty_bucket = st.checkbox(
                    f'I understand this will delete all objects in "{selected_bucket}".'
                )

                # s4a2m5`b1.v1
                # s4a2e6`e1.v1
                if st.button("Confirm Empty Bucket", use_container_width=True):
                    if confirm_empty_bucket:

                        # s4a2m5b1`f1.v1
                        emptied = empty_bucket(selected_bucket)

                        if emptied:
                            # s4a2m5b1f1`m1.v1
                            st.success(
                                f'Bucket "{selected_bucket}" emptied successfully.'
                            )
                        else:
                            # s4a2m5b1f1`m2.v1
                            st.error(
                                f'Bucket "{selected_bucket}" was not emptied.'
                            )
                    else:
                        # s4a2m5b1`m1.v1
                        st.warning("Check the confirmation box first.")

            # s4a2`e7.v1
            elif st.session_state.bucket_action == "delete_bucket":

                # s4a2`m6.v2
                st.warning(
                    f'You are about to delete bucket "{selected_bucket}".'
                )

                # s4a2m6`i1.v1
                confirm_delete_bucket = st.checkbox(
                    f'I understand this will permanently delete bucket "{selected_bucket}".'
                )

                # s4a2m6`b1.v1
                # s4a2e7`e1.v1
                if st.button("Confirm Delete Bucket", use_container_width=True):
                    if confirm_delete_bucket:

                        # s4a2m6b1`f1.v1
                        deleted = delete_bucket(selected_bucket)

                        if deleted:
                            # s4a2m6b1f1`m1.v1
                            st.success(
                                f'Bucket "{selected_bucket}" deleted successfully.'
                            )

                            # s4a2m6b1f1`x1.v1
                            st.session_state.bucket_action = None

                        else:
                            # s4a2m6b1f1`m2.v1
                            st.error(
                                f'Bucket "{selected_bucket}" was not deleted. Empty the bucket first, then try again.'
                            )
                    else:
                        # s4a2m6b1`m1.v1
                        st.warning("Check the confirmation box first.")

    else:

        # s4`m1.v1
        st.error("No buckets found or unable to connect to AWS.")


# ====== s5.v2 - Create Bucket Workflow Section ======
# s5`e1.v1
elif st.session_state.bucket_mode == "create":
    # s5`t1.v1
    st.subheader("Create New Bucket")

    # s5`x1.v1
    # s5e1`e1.v1
    if "recommended_bucket_name" not in st.session_state:
        st.session_state.recommended_bucket_name = "wlm-s3-utils-test-873851887650-us-east-1"

    # s5`i1.v2
    new_bucket_name = st.text_input(
        "New Bucket Name", value=st.session_state.recommended_bucket_name)

    # s5`l1.v1
    region = st.selectbox(
        "AWS Region", ["us-east-1", "us-east-2", "us-west-1", "us-west-2"])

    # s5`b1.v1
    # s5e1`e2.v1
    if st.button("Create Bucket"):
        if new_bucket_name:

            # s5b1`f1.v1
            created = create_bucket(bucket_name=new_bucket_name, region=region)

            if created:
                # s5b1f1`m1.v1
                st.success(f'Bucket "{new_bucket_name}" created successfully.')
            else:
                # s5b1f1`m2.v1
                st.error("Bucket was not created. Check logs or bucket name.")
        else:
            # s5b1`m1.v1
            st.warning("Enter a bucket name first.")
