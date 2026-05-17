import streamlit as st
import base64

from src.aws_operations import (
    bucket_list,
    create_bucket,
    list_files,
    empty_bucket,
    delete_bucket
)


"""
==================================================
IDENTIFIER SYSTEM
==================================================

LEVEL 1
| s | Section

LEVEL 2
| a | Subsection

LEVEL 3
| f | Function Call
| b | Button
| i | Input Element
| l | List / Select Element
| c | Container / Layout
| x | Control Logic / Session State
| m | Message / Status Output
| d | Data Display / Render Output
| t | Text / Titles / Headers
| u | Utility / Helper Logic

LEVEL 4
| v | Version

EXAMPLE BREAKDOWN
-----------------

s4.v1 == Section 4 → Version 1

s4`l1.v1 == Section 4 → List Element 1 → Version 1

s4l1`c1.v1 == Section 4 → List Element 1 → Container 1 → Version 1

s4l1c1`b1.v1 == Section 4 → List Element 1 → Container 1 → Button 1 → Version 1

==================================================
"""


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
if "bucket_mode" not in st.session_state:
    st.session_state.bucket_mode = None

if select_bucket_clicked:
    st.session_state.bucket_mode = "select"

if create_bucket_clicked:
    st.session_state.bucket_mode = "create"


# ====== s4.v3 - Existing Bucket Workflow Section ======
if st.session_state.bucket_mode == "select":
    # s4`t1.v1
    st.subheader("Select Existing Bucket")

    # s4`f1.v1
    buckets = bucket_list()

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
                "List Files", use_container_width=True)

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
        if list_files_clicked:
            st.session_state.bucket_action = "list_files"

        if list_prefixes_clicked:
            st.session_state.bucket_action = "list_prefixes"

        if create_folder_clicked:
            st.session_state.bucket_action = "create_folder"

        if upload_folder_clicked:
            st.session_state.bucket_action = "upload_folder"

        if download_folder_clicked:
            st.session_state.bucket_action = "download_folder"

        if empty_bucket_clicked:
            st.session_state.bucket_action = "empty_bucket"

        if delete_bucket_clicked:
            st.session_state.bucket_action = "delete_bucket"

        # ====== s4`a2.v1 - Bucket Action Output Section ======
        if "bucket_action" in st.session_state:

            if st.session_state.bucket_action == "list_files":

                # s4a2`f1.v2
                files = list_files(selected_bucket)

                if files:

                    # s4a2f1`m1.v2
                    st.success(f"Found {len(files)} file(s).")

                    # s4a2f1`d1.v2
                    for file in files:
                        st.write(file)

                else:

                    # s4a2f1`m2.v2
                    st.warning("No files found in this bucket.")

            elif st.session_state.bucket_action == "list_prefixes":

                # s4a2`m1.v1
                st.info("List folders (prefixes) workflow coming next.")

            elif st.session_state.bucket_action == "create_folder":

                # s4a2`m2.v1
                st.info("Create folder workflow coming next.")

            elif st.session_state.bucket_action == "upload_folder":

                # s4a2`m3.v1
                st.info("Upload folder workflow coming next.")

            elif st.session_state.bucket_action == "download_folder":

                # s4a2`m4.v1
                st.info("Download folder workflow coming next.")

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
elif st.session_state.bucket_mode == "create":
    # s5`t1.v1
    st.subheader("Create New Bucket")

    # s5`x1.v1
    if "recommended_bucket_name" not in st.session_state:
        st.session_state.recommended_bucket_name = "wlm-s3-utils-test-873851887650-us-east-1"

    # s5`i1.v2
    new_bucket_name = st.text_input(
        "New Bucket Name", value=st.session_state.recommended_bucket_name)

    # s5`l1.v1
    region = st.selectbox(
        "AWS Region", ["us-east-1", "us-east-2", "us-west-1", "us-west-2"])

    # s5`b1.v1
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
