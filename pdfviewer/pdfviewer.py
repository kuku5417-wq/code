import streamlit as st
import os
import io
from PIL import Image
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account
from google.auth import default

# ── 페이지 설정 ──────────────────────────────────────────────
st.set_page_config(
    page_title="PDF Viewer",
    page_icon="📄",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── 스타일 ───────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=DM+Mono:wght@400;500&family=DM+Sans:wght@300;400;500&display=swap');

html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif;
}

/* 배경 */
.stApp {
    background: #0f0f0f;
    color: #e8e8e0;
}

/* 사이드바 */
[data-testid="stSidebar"] {
    background: #161616;
    border-right: 1px solid #2a2a2a;
}
[data-testid="stSidebar"] * {
    color: #c8c8c0 !important;
}

/* 헤더 */
.viewer-header {
    font-family: 'DM Serif Display', serif;
    font-size: 2.8rem;
    color: #f0e8d0;
    letter-spacing: -0.02em;
    line-height: 1;
    margin-bottom: 0.2rem;
}
.viewer-sub {
    font-family: 'DM Mono', monospace;
    font-size: 0.75rem;
    color: #666;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    margin-bottom: 2.5rem;
}

/* 폴더 카드 */
.folder-card {
    background: #1a1a1a;
    border: 1px solid #2a2a2a;
    border-radius: 8px;
    padding: 1rem 1.2rem;
    margin-bottom: 0.5rem;
    cursor: pointer;
    transition: border-color 0.2s, background 0.2s;
}
.folder-card:hover {
    border-color: #c8a96e;
    background: #1f1c17;
}

/* 텍스트 뷰어 */
.text-block {
    background: #141414;
    border: 1px solid #252525;
    border-radius: 8px;
    padding: 1.8rem 2rem;
    font-family: 'DM Mono', monospace;
    font-size: 0.82rem;
    line-height: 1.85;
    color: #b8b8b0;
    white-space: pre-wrap;
    word-break: break-word;
    max-height: 500px;
    overflow-y: auto;
}

/* 섹션 라벨 */
.section-label {
    font-family: 'DM Mono', monospace;
    font-size: 0.7rem;
    color: #c8a96e;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    margin-bottom: 0.8rem;
    display: flex;
    align-items: center;
    gap: 0.5rem;
}
.section-label::after {
    content: '';
    flex: 1;
    height: 1px;
    background: #2a2a2a;
}

/* 이미지 카드 */
.img-count {
    font-family: 'DM Mono', monospace;
    font-size: 0.7rem;
    color: #555;
    text-align: center;
    margin-top: 0.3rem;
}

/* 버튼 재스타일 */
.stButton > button {
    background: #1a1a1a !important;
    color: #c8c8c0 !important;
    border: 1px solid #2a2a2a !important;
    border-radius: 6px !important;
    font-family: 'DM Mono', monospace !important;
    font-size: 0.78rem !important;
    letter-spacing: 0.04em !important;
    transition: all 0.2s !important;
}
.stButton > button:hover {
    border-color: #c8a96e !important;
    color: #c8a96e !important;
}

/* 구분선 */
hr {
    border-color: #222 !important;
    margin: 1.5rem 0 !important;
}

/* selectbox */
[data-testid="stSelectbox"] label {
    font-family: 'DM Mono', monospace !important;
    font-size: 0.75rem !important;
    color: #666 !important;
    letter-spacing: 0.06em !important;
    text-transform: uppercase !important;
}
</style>
""", unsafe_allow_html=True)


# ── Google Drive 인증 ────────────────────────────────────────
@st.cache_resource
def get_drive_service():
    """ADC 또는 서비스 계정 JSON으로 Drive 서비스 반환"""
    try:
        creds, _ = default(scopes=["https://www.googleapis.com/auth/drive.readonly"])
        return build("drive", "v3", credentials=creds)
    except Exception as e:
        st.error(f"인증 실패: {e}")
        st.stop()


# ── Drive 유틸 함수 ──────────────────────────────────────────
@st.cache_data(ttl=60)
def list_subfolders(parent_id: str):
    """sample 폴더 내 하위 폴더 목록"""
    svc = get_drive_service()
    res = svc.files().list(
        q=f"'{parent_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false",
        fields="files(id, name, modifiedTime)",
        orderBy="modifiedTime desc"
    ).execute()
    return res.get("files", [])


@st.cache_data(ttl=60)
def list_files_in_folder(folder_id: str):
    """폴더 내 파일 목록"""
    svc = get_drive_service()
    res = svc.files().list(
        q=f"'{folder_id}' in parents and trashed=false",
        fields="files(id, name, mimeType, size)",
        orderBy="name"
    ).execute()
    return res.get("files", [])


@st.cache_data(ttl=300)
def download_file(file_id: str) -> bytes:
    """파일 다운로드 → bytes"""
    svc = get_drive_service()
    req = svc.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, req)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return buf.getvalue()


# ── 폴더 ID 고정 ─────────────────────────────────────────────
SAMPLE_FOLDER_ID = "1iEcz2dXFTQyXtVAINXZzKB9wJfGhyzyU"

# ── 사이드바 ─────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 📁 설정")
    st.markdown("---")
    if st.button("🔄 새로고침"):
        st.cache_data.clear()
        st.rerun()

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        '<div style="font-family:DM Mono,monospace;font-size:0.65rem;color:#444;text-transform:uppercase;letter-spacing:0.1em">PDF Viewer v1.0</div>',
        unsafe_allow_html=True
    )


# ── 메인 헤더 ────────────────────────────────────────────────
st.markdown('<div class="viewer-header">PDF Viewer</div>', unsafe_allow_html=True)
st.markdown('<div class="viewer-sub">Google Drive · Sample Folder</div>', unsafe_allow_html=True)



# ── 폴더 목록 불러오기 ───────────────────────────────────────
with st.spinner("폴더 목록 불러오는 중..."):
    folders = list_subfolders(SAMPLE_FOLDER_ID)

if not folders:
    st.warning("sample 폴더 안에 PDF가 없어요. PDF를 push해보세요!")
    st.stop()


# ── PDF 폴더 선택 ────────────────────────────────────────────
folder_names = [f["name"] for f in folders]
folder_map = {f["name"]: f["id"] for f in folders}

col1, col2 = st.columns([2, 1])
with col1:
    selected_name = st.selectbox(
        "PDF 선택",
        folder_names,
        format_func=lambda x: f"📄  {x}"
    )

selected_folder_id = folder_map[selected_name]

# ── 파일 불러오기 ────────────────────────────────────────────
with st.spinner(f"'{selected_name}' 파일 불러오는 중..."):
    files = list_files_in_folder(selected_folder_id)

text_files = [f for f in files if f["name"] == "text.txt"]
image_files = [f for f in files if f["name"].startswith("image_")]
image_files.sort(key=lambda x: x["name"])

st.markdown("---")

# ── 텍스트 탭 / 이미지 탭 ────────────────────────────────────
tab1, tab2 = st.tabs([f"📝  텍스트", f"🖼️  이미지  ({len(image_files)}장)"])

with tab1:
    st.markdown('<div class="section-label">추출된 텍스트</div>', unsafe_allow_html=True)
    if text_files:
        with st.spinner("텍스트 로딩 중..."):
            text_bytes = download_file(text_files[0]["id"])
            text_content = text_bytes.decode("utf-8", errors="replace")

        if text_content.strip():
            st.markdown(f'<div class="text-block">{text_content}</div>', unsafe_allow_html=True)
            st.download_button(
                "⬇  text.txt 다운로드",
                data=text_bytes,
                file_name=f"{selected_name}_text.txt",
                mime="text/plain"
            )
        else:
            st.info("텍스트가 없거나 이미지 기반 PDF입니다.")
    else:
        st.info("text.txt 파일이 없습니다.")

with tab2:
    st.markdown('<div class="section-label">추출된 이미지</div>', unsafe_allow_html=True)
    if image_files:
        cols_per_row = 3
        rows = [image_files[i:i+cols_per_row] for i in range(0, len(image_files), cols_per_row)]
        for row in rows:
            cols = st.columns(cols_per_row)
            for col, img_file in zip(cols, row):
                with col:
                    with st.spinner(f"{img_file['name']} 로딩 중..."):
                        img_bytes = download_file(img_file["id"])
                    try:
                        img = Image.open(io.BytesIO(img_bytes))
                        st.image(img, use_container_width=True)
                        st.markdown(f'<div class="img-count">{img_file["name"]}</div>', unsafe_allow_html=True)
                        st.download_button(
                            "⬇",
                            data=img_bytes,
                            file_name=img_file["name"],
                            mime="image/png",
                            key=f"dl_{img_file['id']}"
                        )
                    except Exception as e:
                        st.error(f"이미지 로드 실패: {e}")
    else:
        st.info("이미지가 없습니다.")
