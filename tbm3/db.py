"""
데이터 접근 레이어 (Data Access Layer)
구현: MSSQL (SafetyDB) + MySQL (읽기전용)

연결 정보: .streamlit/secrets.toml
  [mssql] server, database, user, password
  [mysql] host, port, database, user, password
  [fileserver] base_path  ← 선택사항 (ref_files, PDF 저장 경로)

테이블 매핑:
  MSSQL SafetyDB : ptw_general, ptw_zlng, weather, tbm_message, tbm_accident, daily_work
  MySQL          : shipinfo, pjtevemt  (읽기전용)

패키지 설치:
  pip install pyodbc pymysql pandas streamlit
  ODBC Driver 17 for SQL Server 설치 필요 (MSSQL 접속용)
"""

from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pyodbc
import pymysql
import streamlit as st

_LOCAL_DIR = Path(__file__).parent / "output"

# pjtevemt 이벤트 → 컬럼 접미사 매핑
# (event_name, col_suffix, is_range)
_EVENT_COL = [
    ("L/C",    "LC",    False),
    ("S/P On", "SP",    False),
    ("B/T",    "BT",    False),
    ("G/T",    "GT",    False),
    ("CMR",    "CMR",   False),
    ("M/T",    "MT",    False),
    ("I/E",    "IE",    False),
    ("A/C",    "AC",    True),
    ("S/T",    "ST",    True),
    ("Cold/T", "COLDT", True),
    ("LNG/B",  "LNGB",  True),
    ("GAS/T",  "GAST",  True),
    ("W/F",    "WF",    False),
    ("D/L",    "DL",    False),
]


# ── DB 연결 ────────────────────────────────────────────────────────────────────

def _mssql_conn():
    s = st.secrets["mssql"]
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={s['server']};DATABASE={s['database']};"
        f"UID={s['user']};PWD={s['password']}"
    )


def _mysql_conn():
    s = st.secrets["mysql"]
    return pymysql.connect(
        host=s["host"], port=int(s["port"]),
        db=s["database"], user=s["user"], password=s["password"],
        charset="utf8mb4", cursorclass=pymysql.cursors.DictCursor
    )


def _mssql_df(query: str, params=None) -> pd.DataFrame:
    conn = _mssql_conn()
    try:
        return pd.read_sql(query, conn, params=params or [])
    finally:
        conn.close()


def _mysql_df(query: str, params=()) -> pd.DataFrame:
    conn = _mysql_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
        return pd.DataFrame(list(rows)) if rows else pd.DataFrame()
    finally:
        conn.close()


def _mssql_exec(query: str, params=None):
    """INSERT / UPDATE / DELETE 실행"""
    conn = _mssql_conn()
    try:
        conn.execute(query, params or [])
        conn.commit()
    finally:
        conn.close()


# ── PTW 조회 ──────────────────────────────────────────────────────────────────

def _filter_ptw(df: pd.DataFrame, date_str: str,
                project: str, team: str, department: str, manager: str) -> pd.DataFrame:
    """start 컬럼 기준 날짜 필터 + 조건 필터"""
    if df.empty:
        return df
    df = df.copy()
    df["_date"] = df["start"].astype(str).str[:10]
    mask = df["_date"] == date_str
    if project:
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
    """일반 PTW(ptw_general) 조회 — MSSQL"""
    df = _mssql_df(
        "SELECT * FROM ptw_general WHERE CAST(start AS DATE) = ?",
        params=[date_str],
    )
    return _filter_ptw(df, date_str, project, team, department, manager)


def get_ptw_zlng(
    date_str: str,
    project: str = None,
    team: str = None,
    department: str = None,
    manager: str = None,
) -> pd.DataFrame:
    """LNG PTW(ptw_zlng) 조회 — MSSQL"""
    df = _mssql_df(
        "SELECT * FROM ptw_zlng WHERE CAST(start AS DATE) = ?",
        params=[date_str],
    )
    return _filter_ptw(df, date_str, project, team, department, manager)


def get_ptwlist(
    date_str: str,
    project: str = None,
    team: str = None,
    department: str = None,
    manager: str = None,
) -> pd.DataFrame:
    """ptw_general + ptw_zlng UNION 조회 (공통 13컬럼) — MSSQL"""
    cols = ("status, category, team, department, manager, "
            "confined, start, finish, project, area, work_detail, work_type")
    query = (
        f"SELECT 'ptw_general' AS source, {cols} FROM ptw_general "
        f"WHERE CAST(start AS DATE) = ? "
        f"UNION ALL "
        f"SELECT 'ptw_zlng' AS source, {cols} FROM ptw_zlng "
        f"WHERE CAST(start AS DATE) = ?"
    )
    df = _mssql_df(query, params=[date_str, date_str])
    return _filter_ptw(df, date_str, project, team, department, manager)


# ── 날씨 조회 ─────────────────────────────────────────────────────────────────

def get_weather(target_date: str) -> dict:
    """
    날씨 정보 조회 — MSSQL weather 테이블
    컬럼: date, day_of_week, day_type, temp_max, temp_min, rainfall, wind_speed
    """
    df = _mssql_df("SELECT * FROM weather WHERE [date] = ?", params=[target_date])
    if df.empty:
        return {}
    return df.iloc[0].to_dict()


# ── 팀장전달사항 ──────────────────────────────────────────────────────────────

def get_message(target_date: str, team: str) -> dict:
    """팀장 전달사항 조회 — MSSQL tbm_message"""
    df = _mssql_df(
        "SELECT TOP 1 content, ref_type, ref_path FROM tbm_message "
        "WHERE msg_date = ? AND team = ? ORDER BY id DESC",
        params=[target_date, team],
    )
    if df.empty:
        return {}
    r = df.iloc[0]
    return {
        "content":  str(r.get("content",  "") or "").strip(),
        "ref_type": str(r.get("ref_type", "") or ""),
        "ref_path": str(r.get("ref_path", "") or ""),
    }


def get_messages_by_team(team: str, limit: int = 30) -> pd.DataFrame:
    """팀별 전달사항 목록 조회 (메뉴 2) — MSSQL tbm_message"""
    return _mssql_df(
        f"SELECT TOP ({limit}) msg_date AS [date], team, content, ref_type, ref_path "
        "FROM tbm_message WHERE team = ? ORDER BY msg_date DESC",
        params=[team],
    )


def save_message(date_str: str, team: str, content: str,
                 ref_type: str = "", ref_path: str = ""):
    """팀장 전달사항 저장 — MSSQL tbm_message INSERT/UPDATE"""
    conn = _mssql_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT id FROM tbm_message WHERE msg_date = ? AND team = ?",
            [date_str, team],
        )
        if cur.fetchone():
            cur.execute(
                "UPDATE tbm_message SET content=?, ref_type=?, ref_path=? "
                "WHERE msg_date=? AND team=?",
                [content, ref_type, ref_path, date_str, team],
            )
        else:
            cur.execute(
                "INSERT INTO tbm_message (msg_date, team, content, ref_type, ref_path) "
                "VALUES (?,?,?,?,?)",
                [date_str, team, content, ref_type, ref_path],
            )
        conn.commit()
    finally:
        conn.close()


def upload_ref_file(file_bytes: bytes, filename: str, mimetype: str) -> str:
    """
    참고자료 파일 저장
    - secrets.toml [fileserver].base_path 있으면 파일서버에 저장
    - 없으면 로컬 output/ref_files/ 저장
    반환값: "ref_files/{filename}" 상대 경로
    """
    try:
        base = Path(st.secrets["fileserver"]["base_path"])
    except Exception:
        base = _LOCAL_DIR
    ref_dir = base / "ref_files"
    ref_dir.mkdir(parents=True, exist_ok=True)
    (ref_dir / filename).write_bytes(file_bytes)
    return f"ref_files/{filename}"


# ── 사고사례 조회 ─────────────────────────────────────────────────────────────

def get_accident_cases(work_type: str, limit: int = 2) -> pd.DataFrame:
    """work_type 기준 유사 사고 사례 조회 — MSSQL tbm_accident"""
    return _mssql_df(
        f"SELECT TOP ({limit}) * FROM tbm_accident "
        "WHERE work_keywords LIKE ? ORDER BY acc_date DESC",
        params=[f"%{work_type}%"],
    )


def get_all_accidents(work_type: str = None) -> pd.DataFrame:
    """사고사례 전체 조회 (메뉴 3) — MSSQL tbm_accident"""
    if work_type:
        return _mssql_df(
            "SELECT * FROM tbm_accident WHERE work_keywords LIKE ? ORDER BY acc_date DESC",
            params=[f"%{work_type}%"],
        )
    return _mssql_df("SELECT * FROM tbm_accident ORDER BY acc_date DESC")


def get_accident_pdf_bytes(pdf_filename: str) -> bytes | None:
    """
    사고사례 PDF 바이너리 반환 (이메일 첨부 / 다운로드)
    - secrets.toml [fileserver].base_path/accident_pdf/ 우선
    - 없으면 로컬 output/accident_pdf/ 시도
    """
    for base in _pdf_bases():
        pdf_path = base / "accident_pdf" / pdf_filename
        if pdf_path.exists():
            return pdf_path.read_bytes()
    return None


def get_accident_pdf_url(pdf_filename: str) -> str | None:
    """
    사고사례 PDF 다운로드 URL 반환
    사내 웹서버 구성 후 아래 패턴으로 교체:
      return f"http://intranet/tbm3/accident_pdf/{pdf_filename}"
    """
    return None


def _pdf_bases() -> list[Path]:
    bases = []
    try:
        bases.append(Path(st.secrets["fileserver"]["base_path"]))
    except Exception:
        pass
    bases.append(_LOCAL_DIR)
    return bases


# ── 호선 정보 ─────────────────────────────────────────────────────────────────

def get_project_info(project: str) -> dict:
    """
    호선 선종 정보 조회 — MySQL shipinfo
    파생 컬럼: FUEL (SHIPTYPE 기반), PROJSEQ (= SEQNO)
    """
    df = _mysql_df(
        "SELECT * FROM shipinfo WHERE SHIPNUM = %s AND STATUS = 'A'",
        params=(project.strip().upper(),),
    )
    if df.empty:
        return {}
    row = df.iloc[0].to_dict()
    shiptype = str(row.get("SHIPTYPE", "") or "").upper()
    row["shipnum"] = row.get("SHIPNUM", "")
    row["FUEL"]    = "LNG" if "LNG" in shiptype else "DF"
    row["PROJSEQ"] = row.get("SEQNO", 1)
    return row


def get_milestones(project: str, target_date: str, days_ahead: int = 14) -> pd.DataFrame:
    """
    호선 시운전 마일스톤 조회 — MySQL pjtevemt
    PERF > PROS > PLAN 우선순위 COALESCE → long format 변환
    범위 이벤트(A/C, S/T, Cold/T, LNG/B, GAS/T): event_date_end 포함
    """
    df = _mysql_df(
        "SELECT * FROM pjtevemt WHERE PJT = %s",
        params=(project.strip().upper(),),
    )
    if df.empty:
        return pd.DataFrame()

    row = df.iloc[0].to_dict()
    rows_out = []

    for event_name, col, is_range in _EVENT_COL:
        if is_range:
            d_from = (row.get(f"PERF{col}FROM") or
                      row.get(f"PROS{col}FROM") or
                      row.get(f"PLAN{col}FROM"))
            d_to   = (row.get(f"PERF{col}TO") or
                      row.get(f"PROS{col}TO") or
                      row.get(f"PLAN{col}TO"))
            if not d_from:
                continue
            if   row.get(f"PERF{col}FROM"): dtype = "실적"
            elif row.get(f"PROS{col}FROM"): dtype = "전망"
            else:                           dtype = "계획"
            rows_out.append({
                "project":        project,
                "event_type":     event_name,
                "event_date":     str(d_from)[:10],
                "event_date_end": str(d_to)[:10] if d_to else None,
                "date_type":      dtype,
            })
        else:
            d = (row.get(f"PERF{col}") or
                 row.get(f"PROS{col}") or
                 row.get(f"PLAN{col}"))
            if not d:
                continue
            if   row.get(f"PERF{col}"): dtype = "실적"
            elif row.get(f"PROS{col}"): dtype = "전망"
            else:                       dtype = "계획"
            rows_out.append({
                "project":        project,
                "event_type":     event_name,
                "event_date":     str(d)[:10],
                "event_date_end": None,
                "date_type":      dtype,
            })

    if not rows_out:
        return pd.DataFrame()

    result = pd.DataFrame(rows_out)
    base     = datetime.strptime(target_date, "%Y-%m-%d").date()
    win_from = base - timedelta(days=days_ahead)
    win_to   = base + timedelta(days=days_ahead)
    result["event_date"] = pd.to_datetime(result["event_date"]).dt.date
    result["_end"] = result["event_date_end"].apply(
        lambda x: pd.to_datetime(x).date() if x else None
    )
    mask_point = (result["event_date"] >= win_from) & (result["event_date"] <= win_to)
    mask_range = (result["_end"].notna() &
                  (result["event_date"] <= win_to) &
                  (result["_end"] >= win_from))
    return (result[mask_point | mask_range]
            .drop(columns=["_end"])
            .sort_values("event_date")
            .reset_index(drop=True))


# ── 부서/팀 목록 ──────────────────────────────────────────────────────────────

def get_all_departments() -> list[str]:
    """부서 목록 조회 (ptw_general + ptw_zlng UNION) — MSSQL"""
    df = _mssql_df(
        "SELECT DISTINCT department FROM ptw_general WHERE department IS NOT NULL "
        "UNION "
        "SELECT DISTINCT department FROM ptw_zlng WHERE department IS NOT NULL"
    )
    if df.empty or "department" not in df.columns:
        return []
    return sorted(df["department"].dropna().unique().tolist())


def get_all_teams() -> list[str]:
    """팀 목록 조회 (ptw_general + ptw_zlng UNION) — MSSQL"""
    df = _mssql_df(
        "SELECT DISTINCT team FROM ptw_general WHERE team IS NOT NULL "
        "UNION "
        "SELECT DISTINCT team FROM ptw_zlng WHERE team IS NOT NULL"
    )
    if df.empty or "team" not in df.columns:
        return []
    return sorted(df["team"].dropna().unique().tolist())


# ── 일상작업 ──────────────────────────────────────────────────────────────────

def get_daily_work() -> pd.DataFrame:
    """일상작업 유형별 위험요소 목록 — MSSQL daily_work"""
    return _mssql_df("SELECT * FROM daily_work ORDER BY work_id")
