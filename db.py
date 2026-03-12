"""
데이터 접근 레이어 (Data Access Layer)

현재 구현: Google Drive CSV
추후 전환: MSSQL (아래 주석 참고)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
MSSQL 전환 가이드 (TODO)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
연결 정보:
  Server   : 192.168.1.100\\SQLEXPRESS
  Database : SafetyDB
  User     : sa
  Password : Admin1234!

패키지 설치:
  pip install pyodbc pandas sqlalchemy

테이블 구조:
  ptw_general   (permit_no, status, category, team, department, manager,
                 confined, start, finish, project, area, work_detail, work_type, ...)
  ptw_zlng      (permit_no, status, category, team, department, manager,
                 confined, start, finish, project, area, work_detail, work_type, ...)
  ptwlist       (source, status, category, team, department, manager,
                 confined, start, finish, project, area, work_detail, work_type)
  weather       (date, day_of_week, day_type, temp_max, temp_min, rainfall, wind_speed)
  message       (date, team, content, ref_type, ref_path)
  accident      (id, date, summary, cause, result, countermeasure,
                 accident_type, work_keywords, source, pdf_filename)

MSSQL 연결 예시:
  import pyodbc
  CONN_STR = (
      "DRIVER={ODBC Driver 17 for SQL Server};"
      "SERVER=192.168.1.100\\\\SQLEXPRESS;"
      "DATABASE=SafetyDB;"
      "UID=sa;PWD=Admin1234!"
  )
  def _get_conn():
      return pyodbc.connect(CONN_STR)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import io
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

FOLDER_ID      = "1pfW8vCXMdOz-2zcmfif-FuHRSm6Zsdl5"  # sample/mssql
_KEY_PATH      = Path(__file__).parent / ".streamlit" / "tbmsample-7eaea86951af.json"
_LOCAL_DIR     = Path(__file__).parent / "output"       # Drive 연결 불가 시 폴백
_EXTRACT_COLS  = ["status", "category", "team", "department", "manager",
                  "confined", "start", "finish", "project", "area",
                  "work_detail", "work_type"]


# ── Google Drive 내부 유틸 ─────────────────────────────────────────────────────
def _get_drive_service(write: bool = False):
    scope = ("https://www.googleapis.com/auth/drive" if write
             else "https://www.googleapis.com/auth/drive.readonly")
    creds = Credentials.from_service_account_file(_KEY_PATH, scopes=[scope])
    return build("drive", "v3", credentials=creds)


# 로컬 우선 읽기 파일 목록
# - message.csv: 앱에서 로컬에만 저장 (Drive write 불가)
_LOCAL_FIRST = {"message.csv"}


def _download_csv(filename: str, folder_id: str = None) -> pd.DataFrame:
    """CSV 로드 → DataFrame
    - message.csv: 로컬 우선 (로컬 없으면 Drive)
    - 그 외: Drive 우선 (실패 시 로컬 폴백)
    """
    local_path = _LOCAL_DIR / filename

    if filename in _LOCAL_FIRST and local_path.exists():
        return pd.read_csv(local_path, encoding="utf-8-sig")
    try:
        svc = _get_drive_service()
        fid = folder_id or FOLDER_ID

        def _search(search_fid: str):
            res = svc.files().list(
                q=f"name='{filename}' and '{search_fid}' in parents and trashed=false",
                fields="files(id)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            ).execute()
            return res.get("files", [])

        files = _search(fid)
        # root 폴더에서 못 찾으면 ptw 하위폴더 시도 (Drive에 ptw/ 서브폴더로 업로드된 경우)
        if not files and folder_id is None:
            ptw_fid = _get_subfolder_id("ptw", FOLDER_ID)
            if ptw_fid:
                files = _search(ptw_fid)

        if not files:
            raise FileNotFoundError(f"{filename} not found in Drive")
        buf = io.BytesIO()
        downloader = MediaIoBaseDownload(buf, svc.files().get_media(fileId=files[0]["id"]))
        done = False
        while not done:
            _, done = downloader.next_chunk()
        buf.seek(0)
        return pd.read_csv(buf, encoding="utf-8-sig")
    except Exception:
        if local_path.exists():
            return pd.read_csv(local_path, encoding="utf-8-sig")
        return pd.DataFrame()


def _upload_csv(df: pd.DataFrame, filename: str, folder_id: str = None):
    """DataFrame → Drive CSV 덮어쓰기 업로드"""
    fid = folder_id or FOLDER_ID
    svc = _get_drive_service(write=True)
    res = svc.files().list(
        q=f"name='{filename}' and '{fid}' in parents and trashed=false",
        fields="files(id)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()
    for f in res.get("files", []):
        svc.files().delete(fileId=f["id"], supportsAllDrives=True).execute()
    buf = io.BytesIO(df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig"))
    svc.files().create(
        body={"name": filename, "parents": [fid]},
        media_body=MediaIoBaseUpload(buf, mimetype="text/csv"),
        fields="id",
        supportsAllDrives=True,
    ).execute()


def _upload_file(file_bytes: bytes, filename: str, mimetype: str,
                 folder_id: str = None) -> str:
    """바이너리 파일 Drive 업로드 → 파일 ID 반환"""
    fid = folder_id or FOLDER_ID
    svc = _get_drive_service(write=True)
    res = svc.files().list(
        q=f"name='{filename}' and '{fid}' in parents and trashed=false",
        fields="files(id)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()
    for f in res.get("files", []):
        svc.files().delete(fileId=f["id"], supportsAllDrives=True).execute()
    result = svc.files().create(
        body={"name": filename, "parents": [fid]},
        media_body=MediaIoBaseUpload(io.BytesIO(file_bytes), mimetype=mimetype),
        fields="id",
        supportsAllDrives=True,
    ).execute()
    return result["id"]


def _get_subfolder_id(name: str, parent_id: str = None) -> str | None:
    """Drive 내 하위 폴더 ID 조회"""
    fid = parent_id or FOLDER_ID
    try:
        svc = _get_drive_service()
        res = svc.files().list(
            q=(f"name='{name}' and '{fid}' in parents "
               f"and mimeType='application/vnd.google-apps.folder' and trashed=false"),
            fields="files(id)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()
        files = res.get("files", [])
        return files[0]["id"] if files else None
    except Exception:
        return None


def get_drive_download_url(file_id: str) -> str:
    """Drive 파일 직접 다운로드 URL"""
    return f"https://drive.google.com/uc?export=download&id={file_id}"


def _get_file_id(filename: str, folder_id: str = None) -> str | None:
    """Drive 폴더 내 파일 ID 조회"""
    fid = folder_id or FOLDER_ID
    svc = _get_drive_service()
    res = svc.files().list(
        q=f"name='{filename}' and '{fid}' in parents and trashed=false",
        fields="files(id)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()
    files = res.get("files", [])
    return files[0]["id"] if files else None


def _download_file_bytes(file_id: str) -> bytes:
    """Drive 파일 바이너리 다운로드"""
    svc = _get_drive_service()
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, svc.files().get_media(fileId=file_id))
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return buf.getvalue()


# ── PTW 조회 ──────────────────────────────────────────────────────────────────

def _filter_ptw(df: pd.DataFrame, date_str: str,
                project: str, team: str, department: str, manager: str) -> pd.DataFrame:
    """start 컬럼 기준으로 날짜 필터 + 선택 조건 필터
    우선순위: project + team + department (1순위) → manager (2순위)
    """
    if df.empty:
        return df
    df = df.copy()
    df["_date"] = df["start"].astype(str).str[:10]
    mask = df["_date"] == date_str
    if project:
        # 숫자만 입력해도 검색 가능 (예: "2657" → "SN2657" 포함 매칭)
        mask &= df["project"].str.upper().str.contains(project.strip().upper(), na=False)
    if team:
        mask &= df["team"] == team
    if department:
        mask &= df["department"] == department
    if manager:
        mask &= df["manager"].str.contains(manager, na=False)
    return df[mask].drop(columns=["_date"]).reset_index(drop=True)


def get_ptw_general(
    date_str: str,
    project: str = None,
    team: str = None,
    department: str = None,
    manager: str = None,
) -> pd.DataFrame:
    """
    일반 PTW(ptw_general) 조회

    # ── MSSQL 전환 시 ──────────────────────────────────────────────────────
    # query = '''
    #     SELECT * FROM ptw_general
    #     WHERE CAST(start AS DATE) = ?
    #       AND (? IS NULL OR project = ?)
    #       AND (? IS NULL OR team = ?)
    #       AND (? IS NULL OR department = ?)
    #       AND (? IS NULL OR manager LIKE ?)
    # '''
    # ────────────────────────────────────────────────────────────────────────
    """
    df = _download_csv("ptw_general.csv")
    return _filter_ptw(df, date_str, project, team, department, manager)


def get_ptw_zlng(
    date_str: str,
    project: str = None,
    team: str = None,
    department: str = None,
    manager: str = None,
) -> pd.DataFrame:
    """
    LNG PTW(ptw_zlng) 조회

    # ── MSSQL 전환 시 ──────────────────────────────────────────────────────
    # query = '''
    #     SELECT * FROM ptw_zlng
    #     WHERE CAST(start AS DATE) = ?
    #       AND (? IS NULL OR project = ?)
    #       AND (? IS NULL OR team = ?)
    #       AND (? IS NULL OR department = ?)
    #       AND (? IS NULL OR manager LIKE ?)
    # '''
    # ────────────────────────────────────────────────────────────────────────
    """
    df = _download_csv("ptw_zlng.csv")
    return _filter_ptw(df, date_str, project, team, department, manager)


def get_ptwlist(
    date_str: str,
    project: str = None,
    team: str = None,
    department: str = None,
    manager: str = None,
) -> pd.DataFrame:
    """
    ptwlist.csv 직접 조회 (Drive에 미리 병합된 파일)
    1순위: project, team, department
    2순위: manager (이름 포함 검색)

    # ── MSSQL 전환 시 ──────────────────────────────────────────────────────
    # query = '''
    #     SELECT * FROM ptwlist
    #     WHERE CAST(start AS DATE) = ?
    #       AND (? IS NULL OR project = ?)
    #       AND (? IS NULL OR team = ?)
    #       AND (? IS NULL OR department = ?)
    #       AND (? IS NULL OR manager LIKE ?)
    # '''
    # ────────────────────────────────────────────────────────────────────────
    """
    df = _download_csv("ptwlist.csv")
    if df.empty:
        return df
    df = df.copy()
    df["_date"] = df["start"].astype(str).str[:10]
    mask = df["_date"] == date_str
    if project:
        # 숫자만 입력해도 검색 가능 (예: "2657" → "SN2657" 포함 매칭)
        mask &= df["project"].str.upper().str.contains(project.strip().upper(), na=False)
    if team:
        mask &= df["team"] == team
    if department:
        mask &= df["department"] == department
    if manager:
        mask &= df["manager"].str.contains(manager, na=False)
    return df[mask].drop(columns=["_date"]).reset_index(drop=True)


# ── 날씨 조회 ─────────────────────────────────────────────────────────────────

def get_weather(target_date: str) -> dict:
    """
    날씨 정보 조회

    # ── MSSQL 전환 시 ──────────────────────────────────────────────────────
    # query = "SELECT * FROM weather WHERE date = ?"
    # df = pd.read_sql(query, conn, params=[target_date])
    # return df.iloc[0].to_dict() if not df.empty else {}
    # ────────────────────────────────────────────────────────────────────────
    """
    df = _download_csv("weather.csv")
    if df.empty:
        return {}
    row = df[df["date"] == target_date]
    return row.iloc[0].to_dict() if not row.empty else {}


# ── 팀장전달사항 ──────────────────────────────────────────────────────────────

def get_message(target_date: str, team: str) -> dict:
    """
    팀장 전달사항 조회 → {content, ref_type, ref_path} 반환
    없으면 빈 dict

    # ── MSSQL 전환 시 ──────────────────────────────────────────────────────
    # query = "SELECT TOP 1 * FROM message WHERE date=? AND team=? ORDER BY rowid DESC"
    # ────────────────────────────────────────────────────────────────────────
    """
    df = _download_csv("message.csv")
    if df.empty:
        return {}
    row = df[(df["date"] == target_date) & (df["team"] == team)]
    if row.empty:
        return {}
    r = row.iloc[-1]  # 마지막 등록 건
    # content 컬럼 우선, 없으면 message 컬럼 폴백 (구버전 CSV 호환)
    content_val = str(r.get("content", "") or "").strip()
    if not content_val:
        content_val = str(r.get("message", "") or "").strip()
    return {
        "content":  content_val,
        "ref_type": str(r.get("ref_type", "") or ""),
        "ref_path": str(r.get("ref_path", "") or ""),
    }


def get_messages_by_team(team: str, limit: int = 30) -> pd.DataFrame:
    """팀별 전달사항 목록 조회 (메뉴 2 조회용)"""
    df = _download_csv("message.csv")
    if df.empty:
        return df
    df = df[df["team"] == team].copy()
    df = df.sort_values("date", ascending=False).head(limit).reset_index(drop=True)
    return df


def save_message(date_str: str, team: str, content: str,
                 ref_type: str = "", ref_path: str = ""):
    """
    팀장 전달사항 저장
    - 로컬 output/message.csv에 우선 저장 (항상 성공)
    - Drive 업로드는 best-effort (서비스 계정 quota 제한으로 실패해도 무시)

    # ── MSSQL 전환 시 ──────────────────────────────────────────────────────
    # query = "INSERT INTO message (date, team, content, ref_type, ref_path) VALUES (?,?,?,?,?)"
    # conn.execute(query, [date_str, team, content, ref_type, ref_path])
    # ────────────────────────────────────────────────────────────────────────
    """
    df = _download_csv("message.csv")
    new_row = {
        "date": date_str, "team": team,
        "content": content, "ref_type": ref_type, "ref_path": ref_path,
    }
    if not df.empty:
        mask = (df["date"] == date_str) & (df["team"] == team)
        if mask.any():
            df.loc[mask, ["content", "ref_type", "ref_path"]] = [content, ref_type, ref_path]
        else:
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    else:
        df = pd.DataFrame([new_row])

    # 로컬 저장 (항상)
    _LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(_LOCAL_DIR / "message.csv", index=False, encoding="utf-8-sig")

    # Drive 업로드 시도 (실패해도 예외 발생 안 함)
    try:
        _upload_csv(df, "message.csv")
    except Exception:
        pass


def upload_ref_file(file_bytes: bytes, filename: str, mimetype: str) -> str:
    """
    참고자료 파일 저장
    - 로컬 output/ref_files/ 에 우선 저장 (항상 성공) → 로컬 경로 반환
    - Drive 업로드는 best-effort (실패 시 로컬 경로 반환)
    """
    ref_dir = _LOCAL_DIR / "ref_files"
    ref_dir.mkdir(parents=True, exist_ok=True)
    (ref_dir / filename).write_bytes(file_bytes)
    local_ref = f"ref_files/{filename}"

    # Drive 업로드 시도
    try:
        svc = _get_drive_service(write=True)
        res = svc.files().list(
            q=(f"name='ref_files' and '{FOLDER_ID}' in parents "
               f"and mimeType='application/vnd.google-apps.folder' and trashed=false"),
            fields="files(id)",
            supportsAllDrives=True, includeItemsFromAllDrives=True,
        ).execute()
        folders = res.get("files", [])
        if folders:
            ref_folder_id = folders[0]["id"]
        else:
            f = svc.files().create(
                body={"name": "ref_files",
                      "mimeType": "application/vnd.google-apps.folder",
                      "parents": [FOLDER_ID]},
                fields="id", supportsAllDrives=True,
            ).execute()
            ref_folder_id = f["id"]
        return _upload_file(file_bytes, filename, mimetype, ref_folder_id)
    except Exception:
        return local_ref


# ── 사고사례 조회 ─────────────────────────────────────────────────────────────

def get_accident_cases(work_type: str, limit: int = 2) -> pd.DataFrame:
    """
    work_type 기준 유사 사고 사례 조회 (pdf_filename 포함)

    # ── MSSQL 전환 시 ──────────────────────────────────────────────────────
    # query = f"SELECT TOP {limit} * FROM accident WHERE work_keywords LIKE ? ORDER BY date DESC"
    # return pd.read_sql(query, conn, params=[f'%{work_type}%'])
    # ────────────────────────────────────────────────────────────────────────
    """
    df = _download_csv("accident.csv")
    if df.empty:
        return df
    mask = df["work_keywords"].str.contains(work_type, na=False)
    return df[mask].head(limit).reset_index(drop=True)


def get_all_accidents(work_type: str = None) -> pd.DataFrame:
    """사고사례 전체 조회 (메뉴 3 조회용)"""
    df = _download_csv("accident.csv")
    if df.empty:
        return df
    if work_type:
        df = df[df["work_keywords"].str.contains(work_type, na=False)]
    return df.reset_index(drop=True)


def _get_accident_pdf_folder_id() -> str | None:
    """accident_pdf 폴더 ID 조회 (ptw/ 하위 → root 순서로 탐색)"""
    # ptw/accident_pdf 먼저 시도
    ptw_fid = _get_subfolder_id("ptw", FOLDER_ID)
    if ptw_fid:
        fid = _get_subfolder_id("accident_pdf", ptw_fid)
        if fid:
            return fid
    # root/accident_pdf 시도
    return _get_subfolder_id("accident_pdf", FOLDER_ID)


def get_accident_pdf_bytes(pdf_filename: str) -> bytes | None:
    """
    사고사례 PDF 파일 바이너리 반환 (이메일 첨부용)
    Drive ptw/accident_pdf/ 또는 accident_pdf/ 폴더에서 다운로드
    """
    pdf_folder_id = _get_accident_pdf_folder_id()
    if not pdf_folder_id:
        return None
    file_id = _get_file_id(pdf_filename, pdf_folder_id)
    if not file_id:
        return None
    return _download_file_bytes(file_id)


def get_accident_pdf_url(pdf_filename: str) -> str | None:
    """사고사례 PDF Drive 다운로드 URL 반환"""
    pdf_folder_id = _get_accident_pdf_folder_id()
    if not pdf_folder_id:
        return None
    file_id = _get_file_id(pdf_filename, pdf_folder_id)
    if not file_id:
        return None
    return get_drive_download_url(file_id)


# ── 호선 정보 ─────────────────────────────────────────────────────────────────

def get_project_info(project: str) -> dict:
    """
    호선 선종 정보 조회 (MySQL shipinfo 기반 pjtlist)

    # ── MySQL 전환 시 ────────────────────────────────────────────────────────
    # query = "SELECT shipnum,MAINHULLNUM,SEQNO,TITLE,IMO,SHIPTYPE,REGOWNER,TYPEMODEL,FUEL,PROJSEQ"
    #         " FROM shipinfo WHERE shipnum = %s"
    # ────────────────────────────────────────────────────────────────────────
    """
    df = _download_csv("pjtlist.csv")
    if df.empty:
        return {}
    row = df[df["shipnum"].str.upper() == project.strip().upper()]
    return row.iloc[0].to_dict() if not row.empty else {}


def get_milestones(project: str, target_date: str, days_ahead: int = 14) -> pd.DataFrame:
    """
    호선 시운전 마일스톤 조회 (target_date ± days_ahead)
    범위 이벤트(S/T, Cold/T, Gas/T): event_date_end 컬럼 포함
    대상 기간과 겹치는 이벤트 모두 포함 (진행 중인 범위 이벤트 포함)

    # ── MySQL 전환 시 (pjtevemt 테이블 기반) ─────────────────────────────────
    # PERF > PRO > PLAN 우선순위로 날짜 추출하여 milestone 뷰 생성
    # 컬럼 패턴: PLAN{EVENT}_{FROM|TO}, PRO{EVENT}_{FROM|TO}, PERF{EVENT}_{FROM|TO}
    # PJT 컬럼 = 호선번호(shipnum)
    # query = '''
    #     SELECT project, event_type, event_date, event_date_end, date_type
    #     FROM v_project_milestone
    #     WHERE project = %s
    #       AND (event_date BETWEEN DATE_SUB(%s, INTERVAL %s DAY) AND DATE_ADD(%s, INTERVAL %s DAY)
    #            OR (event_date_end IS NOT NULL
    #                AND event_date <= %s
    #                AND event_date_end >= DATE_SUB(%s, INTERVAL %s DAY)))
    #     ORDER BY event_date
    # '''
    # ────────────────────────────────────────────────────────────────────────
    """
    df = _download_csv("project_milestone.csv")
    if df.empty:
        return df
    df = df[df["project"].str.upper() == project.strip().upper()].copy()
    if df.empty:
        return df
    base = datetime.strptime(target_date, "%Y-%m-%d").date()
    df["event_date"] = pd.to_datetime(df["event_date"]).dt.date
    df["event_date_end"] = df["event_date_end"].fillna("").astype(str)
    df["_end"] = df["event_date_end"].apply(
        lambda x: pd.to_datetime(x).date() if x.strip() else None
    )
    win_from = base - timedelta(days=days_ahead)
    win_to   = base + timedelta(days=days_ahead)
    # 단일 이벤트: 윈도우 내 event_date
    mask_point = (df["event_date"] >= win_from) & (df["event_date"] <= win_to)
    # 범위 이벤트: 윈도우와 겹치는 경우 (진행 중 포함)
    mask_range = (df["_end"].notna() &
                  (df["event_date"] <= win_to) &
                  (df["_end"] >= win_from))
    return (df[mask_point | mask_range]
            .drop(columns=["_end"])
            .sort_values("event_date")
            .reset_index(drop=True))


# ── 부서/팀 목록 ──────────────────────────────────────────────────────────────

def get_all_departments() -> list[str]:
    """
    부서 목록 조회 (ptwlist.csv 기반 — Drive 1회 호출)

    # ── MSSQL 전환 시 ──────────────────────────────────────────────────────
    # query = "SELECT DISTINCT department FROM ptwlist ORDER BY department"
    # ────────────────────────────────────────────────────────────────────────
    """
    df = _download_csv("ptwlist.csv")
    if df.empty or "department" not in df.columns:
        return []
    return sorted(df["department"].dropna().unique().tolist())


def get_all_teams() -> list[str]:
    """
    팀 목록 조회 (ptwlist.csv 기반 — Drive 1회 호출)

    # ── MSSQL 전환 시 ──────────────────────────────────────────────────────
    # query = "SELECT DISTINCT team FROM ptwlist ORDER BY team"
    # ────────────────────────────────────────────────────────────────────────
    """
    df = _download_csv("ptwlist.csv")
    if df.empty or "team" not in df.columns:
        return []
    return sorted(df["team"].dropna().unique().tolist())


def get_daily_work() -> pd.DataFrame:
    """
    일상작업 유형별 위험요소 목록 전체 반환 (PTW 없는 경우 폴백)
    컬럼: work_id, work_category, work_detail, risk_hint
    앱에서 랜덤 2개 선택하여 스크립트 제시

    # ── MSSQL 전환 시 ──────────────────────────────────────────────────────
    # query = "SELECT * FROM daily_work ORDER BY work_id"
    # ────────────────────────────────────────────────────────────────────────
    """
    return _download_csv("daily_work.csv")
