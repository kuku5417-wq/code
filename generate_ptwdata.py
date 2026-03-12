import io
import json
import random
import urllib.request
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

MSSQL_FOLDER_ID = "1pfW8vCXMdOz-2zcmfif-FuHRSm6Zsdl5"  # sample/mssql ← MSSQL 서버 데이터
MYSQL_FOLDER_ID  = "TODO_MYSQL_FOLDER_ID"              # sample/mysql ← MySQL 서버 데이터 (폴더 생성 후 기입)
KEY_PATH = Path(__file__).parent / ".streamlit" / "tbmsample-7eaea86951af.json"

random.seed(42)

# ── 기준 데이터 ────────────────────────────────────────────────────────────────
# project_list.csv 기준: LNG FPSO → ptw_zlng, 나머지 → ptw_general
FPSO_PROJECTS    = ["SN2703", "SN2802"]          # LNG FPSO → offshore PTW (ptw_zlng)
GENERAL_PROJECTS = ["SN2657", "SN2658", "SN2701",
                    "SN2702", "SN2704", "SN2705",
                    "SN2706", "SN2801"]            # 나머지 → ptw_general

TEAMS = ["A팀", "B팀"]
DEPARTMENTS = ["A1", "A2", "B1"]

MANAGERS = ["김철수", "이영호", "박민준", "최동훈", "정재원", "강성진",
            "오준혁", "장민석", "백승현", "김도현", "이상훈", "박진우"]

SUPERVISORS = ["윤태현", "임성민", "한정호", "신동현", "오성민",
               "류재현", "홍길동", "권순호", "배진수", "문성호"]

AREAS = ["ER 2nd", "ER 3rd deck", "ER 하부 플랫폼", "펌프룸", "이중저 탱크",
         "브릿지 덱", "Steering gear room", "화물창 1번", "화물창 2번",
         "기관실 상부", "선수부", "선미부"]

WORK_DETAILS_GENERAL = [
    ("발전기 처음 돌림", "발전기가동"),
    ("메인엔진 시운전", "시운전작업"),
    ("밸브 압력 테스트", "고압작업"),
    ("연료유 탱크 청소", "밀폐작업"),
    ("전기 배선 점검", "전기작업"),
    ("냉각수 계통 점검", "시운전작업"),
    ("보조기기 기능시험", "시운전작업"),
    ("공기압 시스템 점검", "고압작업"),
    ("화재 감지 시스템 점검", "전기작업"),
    ("비상발전기 부하시험", "발전기가동"),
    ("조타장치 기능시험", "시운전작업"),
    ("펌프룸 배관 점검", "밀폐작업"),
]

WORK_DETAILS_ZLNG = [
    ("LNG 카고탱크 점검", "LNG작업"),
    ("가스 공급 계통 시험", "LNG작업"),
    ("극저온 밸브 점검", "LNG작업"),
    ("LNG 재기화 설비 시운전", "LNG작업"),
    ("가스 검지기 캘리브레이션", "LNG작업"),
    ("보냉재 점검", "밀폐작업"),
    ("LNG 펌프 기능시험", "LNG작업"),
    ("가스 배관 기밀시험", "고압작업"),
    ("비상차단밸브 작동시험", "LNG작업"),
    ("LNG 탱크 압력 조정", "고압작업"),
]

SUBSYSTEM_DESCS = [
    "Main Gas Supply System", "Fuel Gas Supply System",
    "Cargo Containment System", "Reliquefaction System",
    "Emergency Shutdown System", "Gas Detection System",
]

SCOPES = ["정기점검", "초도점검", "특별점검", "수리", "교체"]

CONTACTS = [f"010-{random.randint(1000,9999)}-{random.randint(1000,9999)}" for _ in range(20)]
random.seed(42)
CONTACTS = [f"010-{str(random.randint(1000,9999))}-{str(random.randint(1000,9999))}" for _ in range(20)]

COORDINATOR_COMMENTS = ["이상없음", "안전조치 완료", "현장확인 완료", "추가검토 필요", "승인"]
OVERTIME_REASONS = ["공정지연", "납기일정", "긴급수리", ""]

# ── 날짜 범위 ──────────────────────────────────────────────────────────────────
START_DATE = date(2026, 1, 1)
END_DATE   = date(2026, 3, 11)

all_days = []
d = START_DATE
while d <= END_DATE:
    all_days.append(d)
    d += timedelta(days=1)

working_days = [d for d in all_days if d.weekday() < 5]  # 평일만


# ── ptw_general 생성 ───────────────────────────────────────────────────────────
general_rows = []
pid = 1

for d in working_days:
    # 하루 3~6건
    count = random.randint(3, 6)
    for _ in range(count):
        start_hour = random.choice([8, 9])
        finish_hour = random.choice([17, 18])
        start_dt = f"{d.strftime('%Y-%m-%d')} {start_hour:02d}:00"
        finish_dt = f"{d.strftime('%Y-%m-%d')} {finish_hour:02d}:00"
        work_detail, work_type = random.choice(WORK_DETAILS_GENERAL)
        team = random.choice(TEAMS)
        dept = random.choice(DEPARTMENTS)
        mgr = random.choice(MANAGERS)
        supervisor = random.choice(SUPERVISORS)
        confined = "밀폐" if work_type == "밀폐작업" else "일반"
        status = random.choices(["승인", "취소"], weights=[85, 15])[0]
        category = random.choices(["A", "B", "C"], weights=[20, 50, 30])[0]
        mixed_date = f"{d.strftime('%Y-%m-%d')} {random.randint(8,12):02d}:00" if random.random() > 0.5 else ""
        overtime = random.choice(OVERTIME_REASONS) if random.random() > 0.8 else ""

        general_rows.append({
            "permit_no":            f"PG{pid:04d}",
            "status":               status,
            "category":             category,
            "safety_check":         random.choice(["완료", "미완료"]),
            "safety_checker":       random.choice(SUPERVISORS),
            "team":                 team,
            "department":           dept,
            "manager":              mgr,
            "contact":              random.choice(CONTACTS),
            "sub_contractor":       f"{dept} 협력사",
            "sub_manager":          random.choice(MANAGERS),
            "sub_contact":          random.choice(CONTACTS),
            "supervisor":           supervisor,
            "workers":              random.randint(2, 8),
            "location":             random.choice(AREAS),
            "confined":             confined,
            "category_type":        random.choice(["일반", "특수"]),
            "start":                start_dt,
            "finish":               finish_dt,
            "project":              random.choice(GENERAL_PROJECTS),
            "area":                 random.choice(AREAS),
            "work_detail":          work_detail,
            "coordinator_comment":  random.choice(COORDINATOR_COMMENTS),
            "mixed_approval_date":  mixed_date,
            "overtime_reason":      overtime,
            "work_type":            work_type,
        })
        pid += 1

general_df = pd.DataFrame(general_rows)


# ── ptw_zlng 생성 ──────────────────────────────────────────────────────────────
zlng_rows = []
zid = 1

for d in working_days:
    # LNG 작업은 하루 1~3건
    count = random.randint(1, 3)
    for _ in range(count):
        start_hour = random.choice([8, 9])
        finish_hour = random.choice([17, 18])
        start_dt = f"{d.strftime('%Y-%m-%d')} {start_hour:02d}:00"
        finish_dt = f"{d.strftime('%Y-%m-%d')} {finish_hour:02d}:00"
        work_detail, work_type = random.choice(WORK_DETAILS_ZLNG)
        team = random.choice(TEAMS)
        dept = random.choice(DEPARTMENTS)
        mgr = random.choice(MANAGERS)
        supervisor = random.choice(SUPERVISORS)
        confined = "밀폐" if work_type == "밀폐작업" else "일반"
        status = random.choices(["승인", "취소"], weights=[85, 15])[0]
        category = random.choices(["A", "B", "C"], weights=[30, 50, 20])[0]

        zlng_rows.append({
            "approval":             random.choice(["승인", "미승인"]),
            "status":               status,
            "category":             category,
            "permit_no":            f"ZL{zid:04d}",
            "department":           dept,
            "manager":              mgr,
            "contact":              random.choice(CONTACTS),
            "supervisor":           supervisor,
            "confined":             confined,
            "category_type":        random.choice(["일반", "특수"]),
            "start":                start_dt,
            "finish":               finish_dt,
            "project":              random.choice(FPSO_PROJECTS),
            "area":                 random.choice(AREAS),
            "work_detail":          work_detail,
            "equipment_owner_1st":  random.choice(MANAGERS),
            "equipment_owner_2nd":  random.choice(MANAGERS),
            "system_no":            f"SYS-{random.randint(100,999)}",
            "subsystem_desc":       random.choice(SUBSYSTEM_DESCS),
            "scope":                random.choice(SCOPES),
            "site_check":           random.choice(["완료", "미완료"]),
            "icc_no":               f"ICC-{random.randint(1000,9999)}",
            "team":                 team,
            "work_type":            work_type,
        })
        zid += 1

zlng_df = pd.DataFrame(zlng_rows)


# ── ptwlist 생성 (두 테이블 병합, 추출 컬럼) ───────────────────────────────────
EXTRACT_COLS = ["status", "category", "team", "department", "manager",
                "confined", "start", "finish", "project", "area",
                "work_detail", "work_type"]

general_list = general_df[EXTRACT_COLS].copy()
general_list.insert(0, "source", "ptw_general")

zlng_list = zlng_df[EXTRACT_COLS].copy()
zlng_list.insert(0, "source", "ptw_zlng")

ptwlist_df = pd.concat([general_list, zlng_list], ignore_index=True)


# ── pjtlist 생성 (MySQL shipinfo 기반 샘플) ────────────────────────────────
# 실제 전환 시:
#   SELECT SHIPNUM, MAINHULLNUM, SEQNO, TITLE, IMO, SHIPTYPE, REGOWNER, TYPEMODEL, SHIPCLASS, DOCK
#   FROM shipinfo WHERE STATUS='A'
# ※ FUEL   : shipinfo에 없음 → SHIPTYPE 기반 파생 (LNG/ETH → "LNG", 나머지 → "DF")
# ※ PROJSEQ: shipinfo에 없음 → 앱 표시용 순번 (SELECT ROW_NUMBER() OVER(ORDER BY SHIPNUM) 또는 SEQNO 활용)
# ※ SEQNO  : int(11)
PJTLIST_ROWS = [
    {"shipnum":"SN2657","MAINHULLNUM":"H2657","SEQNO":1,"TITLE":"15000TEU Container Vessel",
     "IMO":"IMO9876501","SHIPTYPE":"CNT","REGOWNER":"EVERGREEN MARINE","TYPEMODEL":"15000 TEU","FUEL":"DF","PROJSEQ":1},
    {"shipnum":"SN2658","MAINHULLNUM":"H2658","SEQNO":2,"TITLE":"15000TEU Container Vessel",
     "IMO":"IMO9876502","SHIPTYPE":"CNT","REGOWNER":"EVERGREEN MARINE","TYPEMODEL":"15000 TEU","FUEL":"DF","PROJSEQ":2},
    {"shipnum":"SN2701","MAINHULLNUM":"H2701","SEQNO":3,"TITLE":"174K LNG Carrier",
     "IMO":"IMO9876503","SHIPTYPE":"LNG","REGOWNER":"KOREA GAS","TYPEMODEL":"174K","FUEL":"LNG","PROJSEQ":3},
    {"shipnum":"SN2702","MAINHULLNUM":"H2702","SEQNO":4,"TITLE":"174K LNG Carrier",
     "IMO":"IMO9876504","SHIPTYPE":"LNG","REGOWNER":"KOREA GAS","TYPEMODEL":"174K","FUEL":"LNG","PROJSEQ":4},
    {"shipnum":"SN2703","MAINHULLNUM":"H2703","SEQNO":5,"TITLE":"LNG FPSO 300K",
     "IMO":"IMO9876505","SHIPTYPE":"LNG","REGOWNER":"SHELL","TYPEMODEL":"LNG FPSO 300K","FUEL":"LNG","PROJSEQ":5},
    {"shipnum":"SN2704","MAINHULLNUM":"H2704","SEQNO":6,"TITLE":"98K Ethane Carrier",
     "IMO":"IMO9876506","SHIPTYPE":"ETH","REGOWNER":"NAVIGATOR GAS","TYPEMODEL":"98K","FUEL":"LNG","PROJSEQ":6},
    {"shipnum":"SN2705","MAINHULLNUM":"H2705","SEQNO":7,"TITLE":"24000TEU Container Vessel",
     "IMO":"IMO9876507","SHIPTYPE":"CNT","REGOWNER":"MSC","TYPEMODEL":"24000 TEU","FUEL":"DF","PROJSEQ":7},
    {"shipnum":"SN2706","MAINHULLNUM":"H2706","SEQNO":8,"TITLE":"180K LNG Carrier",
     "IMO":"IMO9876508","SHIPTYPE":"LNG","REGOWNER":"TOTALENERGIES","TYPEMODEL":"180K","FUEL":"LNG","PROJSEQ":8},
    {"shipnum":"SN2801","MAINHULLNUM":"H2801","SEQNO":9,"TITLE":"98K Ethane Carrier",
     "IMO":"IMO9876509","SHIPTYPE":"ETH","REGOWNER":"NAVIGATOR GAS","TYPEMODEL":"98K","FUEL":"LNG","PROJSEQ":9},
    {"shipnum":"SN2802","MAINHULLNUM":"H2802","SEQNO":10,"TITLE":"LNG FPSO 300K",
     "IMO":"IMO9876510","SHIPTYPE":"LNG","REGOWNER":"BP","TYPEMODEL":"LNG FPSO 300K","FUEL":"LNG","PROJSEQ":10},
]
pjtlist_df = pd.DataFrame(PJTLIST_ROWS)

# ── shipinfo 생성 (실제 테이블 전체 컬럼 구조) ────────────────────────────────
# UID: auto_increment → 생략 (DB 삽입 시 자동 생성)
# INSERTBY/UPDATEBY: 1 (시스템 계정)
# STATUS: 'A' (활성)
_NOW = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
SHIPINFO_ROWS = [
    {"SHIPNUM":"SN2657","MAINHULLNUM":"H2657","SEQNO":1,"TITLE":"15000TEU Container Vessel",
     "IMO":"IMO9876501","MMSI":"440100001","SHIPTYPE":"CNT","REGOWNER":"EVERGREEN MARINE",
     "BUILTDATE":"2026","FLAG":"PAN","GROSSTON":"149000","DWT":"141000",
     "LOA":"366","DRAUGHT":"16.0","BUILDER":"HHI","BUILTBY":"현대중공업",
     "WORKFINISH":"2026-02-28","ISDEFAULT":"Y","STATUS":"A",
     "INSERTBY":1,"INSERTDATE":_NOW,"UPDATEBY":1,"UPDATEDATE":_NOW,
     "SHIPCLASS":"KR","TYPEMODEL":"15000 TEU","DOCK":"D1","LOC":"1도크"},
    {"SHIPNUM":"SN2658","MAINHULLNUM":"H2657","SEQNO":2,"TITLE":"15000TEU Container Vessel",
     "IMO":"IMO9876502","MMSI":"440100002","SHIPTYPE":"CNT","REGOWNER":"EVERGREEN MARINE",
     "BUILTDATE":"2026","FLAG":"PAN","GROSSTON":"149000","DWT":"141000",
     "LOA":"366","DRAUGHT":"16.0","BUILDER":"HHI","BUILTBY":"현대중공업",
     "WORKFINISH":"2026-04-30","ISDEFAULT":"N","STATUS":"A",
     "INSERTBY":1,"INSERTDATE":_NOW,"UPDATEBY":1,"UPDATEDATE":_NOW,
     "SHIPCLASS":"KR","TYPEMODEL":"15000 TEU","DOCK":"D1","LOC":"1도크"},
    {"SHIPNUM":"SN2701","MAINHULLNUM":"H2701","SEQNO":3,"TITLE":"174K LNG Carrier",
     "IMO":"IMO9876503","MMSI":"440200001","SHIPTYPE":"LNG","REGOWNER":"KOREA GAS",
     "BUILTDATE":"2026","FLAG":"KOR","GROSSTON":"95000","DWT":"77000",
     "LOA":"295","DRAUGHT":"12.5","BUILDER":"HHI","BUILTBY":"현대중공업",
     "WORKFINISH":"2026-03-31","ISDEFAULT":"Y","STATUS":"A",
     "INSERTBY":1,"INSERTDATE":_NOW,"UPDATEBY":1,"UPDATEDATE":_NOW,
     "SHIPCLASS":"LR","TYPEMODEL":"174K","DOCK":"D2","LOC":"2도크"},
    {"SHIPNUM":"SN2702","MAINHULLNUM":"H2701","SEQNO":4,"TITLE":"174K LNG Carrier",
     "IMO":"IMO9876504","MMSI":"440200002","SHIPTYPE":"LNG","REGOWNER":"KOREA GAS",
     "BUILTDATE":"2026","FLAG":"KOR","GROSSTON":"95000","DWT":"77000",
     "LOA":"295","DRAUGHT":"12.5","BUILDER":"HHI","BUILTBY":"현대중공업",
     "WORKFINISH":"2026-06-30","ISDEFAULT":"N","STATUS":"A",
     "INSERTBY":1,"INSERTDATE":_NOW,"UPDATEBY":1,"UPDATEDATE":_NOW,
     "SHIPCLASS":"LR","TYPEMODEL":"174K","DOCK":"D2","LOC":"2도크"},
    {"SHIPNUM":"SN2703","MAINHULLNUM":"H2703","SEQNO":5,"TITLE":"LNG FPSO 300K",
     "IMO":"IMO9876505","MMSI":"440300001","SHIPTYPE":"LNG","REGOWNER":"SHELL",
     "BUILTDATE":"2026","FLAG":"GBR","GROSSTON":"170000","DWT":"300000",
     "LOA":"488","DRAUGHT":"14.0","BUILDER":"HHI","BUILTBY":"현대중공업",
     "WORKFINISH":"2026-01-31","ISDEFAULT":"Y","STATUS":"A",
     "INSERTBY":1,"INSERTDATE":_NOW,"UPDATEBY":1,"UPDATEDATE":_NOW,
     "SHIPCLASS":"DNV","TYPEMODEL":"LNG FPSO 300K","DOCK":"D3","LOC":"3도크"},
    {"SHIPNUM":"SN2704","MAINHULLNUM":"H2704","SEQNO":6,"TITLE":"98K Ethane Carrier",
     "IMO":"IMO9876506","MMSI":"440400001","SHIPTYPE":"ETH","REGOWNER":"NAVIGATOR GAS",
     "BUILTDATE":"2026","FLAG":"MLT","GROSSTON":"56000","DWT":"56000",
     "LOA":"230","DRAUGHT":"11.5","BUILDER":"HHI","BUILTBY":"현대중공업",
     "WORKFINISH":"2026-05-31","ISDEFAULT":"Y","STATUS":"A",
     "INSERTBY":1,"INSERTDATE":_NOW,"UPDATEBY":1,"UPDATEDATE":_NOW,
     "SHIPCLASS":"BV","TYPEMODEL":"98K","DOCK":"D2","LOC":"2도크"},
    {"SHIPNUM":"SN2705","MAINHULLNUM":"H2705","SEQNO":7,"TITLE":"24000TEU Container Vessel",
     "IMO":"IMO9876507","MMSI":"440100003","SHIPTYPE":"CNT","REGOWNER":"MSC",
     "BUILTDATE":"2026","FLAG":"PAN","GROSSTON":"228000","DWT":"235000",
     "LOA":"400","DRAUGHT":"16.5","BUILDER":"HHI","BUILTBY":"현대중공업",
     "WORKFINISH":"2026-07-31","ISDEFAULT":"Y","STATUS":"A",
     "INSERTBY":1,"INSERTDATE":_NOW,"UPDATEBY":1,"UPDATEDATE":_NOW,
     "SHIPCLASS":"KR","TYPEMODEL":"24000 TEU","DOCK":"D1","LOC":"1도크"},
    {"SHIPNUM":"SN2706","MAINHULLNUM":"H2706","SEQNO":8,"TITLE":"180K LNG Carrier",
     "IMO":"IMO9876508","MMSI":"440200003","SHIPTYPE":"LNG","REGOWNER":"TOTALENERGIES",
     "BUILTDATE":"2026","FLAG":"FRA","GROSSTON":"100000","DWT":"80000",
     "LOA":"300","DRAUGHT":"12.8","BUILDER":"HHI","BUILTBY":"현대중공업",
     "WORKFINISH":"2026-04-30","ISDEFAULT":"Y","STATUS":"A",
     "INSERTBY":1,"INSERTDATE":_NOW,"UPDATEBY":1,"UPDATEDATE":_NOW,
     "SHIPCLASS":"LR","TYPEMODEL":"180K","DOCK":"D3","LOC":"3도크"},
    {"SHIPNUM":"SN2801","MAINHULLNUM":"H2801","SEQNO":9,"TITLE":"98K Ethane Carrier",
     "IMO":"IMO9876509","MMSI":"440400002","SHIPTYPE":"ETH","REGOWNER":"NAVIGATOR GAS",
     "BUILTDATE":"2026","FLAG":"MLT","GROSSTON":"56000","DWT":"56000",
     "LOA":"230","DRAUGHT":"11.5","BUILDER":"HHI","BUILTBY":"현대중공업",
     "WORKFINISH":"2026-08-31","ISDEFAULT":"N","STATUS":"A",
     "INSERTBY":1,"INSERTDATE":_NOW,"UPDATEBY":1,"UPDATEDATE":_NOW,
     "SHIPCLASS":"BV","TYPEMODEL":"98K","DOCK":"D2","LOC":"2도크"},
    {"SHIPNUM":"SN2802","MAINHULLNUM":"H2802","SEQNO":10,"TITLE":"LNG FPSO 300K",
     "IMO":"IMO9876510","MMSI":"440300002","SHIPTYPE":"LNG","REGOWNER":"BP",
     "BUILTDATE":"2026","FLAG":"GBR","GROSSTON":"170000","DWT":"300000",
     "LOA":"488","DRAUGHT":"14.0","BUILDER":"HHI","BUILTBY":"현대중공업",
     "WORKFINISH":"2026-06-30","ISDEFAULT":"N","STATUS":"A",
     "INSERTBY":1,"INSERTDATE":_NOW,"UPDATEBY":1,"UPDATEDATE":_NOW,
     "SHIPCLASS":"ABS","TYPEMODEL":"LNG FPSO 300K","DOCK":"D3","LOC":"3도크"},
]
shipinfo_df = pd.DataFrame(SHIPINFO_ROWS)


# ── project_milestone 생성 (MySQL pjtevemt 기반 샘플) ─────────────────────
# 실제 전환 시 (우선순위: 실적 > 전망 > 계획):
#   COALESCE(PERFLC,       PROSLC,       PLANLC)       AS lc
#   COALESCE(PERFSP,       PROSSP,       PLANSP)       AS sp
#   COALESCE(PERFBT,       PROSBT,       PLANBT)       AS bt
#   COALESCE(PERFGT,       PROSGT,       PLANGT)       AS gt
#   COALESCE(PERFCMR,      PROSCMR,      PLANCMR)      AS cmr
#   COALESCE(PERFMT,       PROSMT,       PLANMT)       AS mt
#   COALESCE(PERFIE,       PROSIE,       PLANIE)       AS ie
#   COALESCE(PERFACFROM,   PROSACFROM,   PLANACFROM)   AS ac_from
#   COALESCE(PERFACTO,     PROSACTO,     PLANACTO)     AS ac_to
#   COALESCE(PERFSTFROM,   PROSSTFROM,   PLANSTFROM)   AS st_from
#   COALESCE(PERFSTTO,     PROSSTTO,     PLANSTTO)     AS st_to
#   COALESCE(PERFCOLDFROM, PROSCOLDFROM, PLANCOLDFROM) AS cold_from   -- LNG only
#   COALESCE(PERFCOLDTO,   PROSCOLDTO,   PLANCOLDTO)   AS cold_to     -- LNG only
#   COALESCE(PERFLNGFROM,  PROSLNGFROM,  PLANLNGFROM)  AS lng_from    -- LNG only
#   COALESCE(PERFLNGTO,    PROSLNGTO,    PLANLNGTO)    AS lng_to      -- LNG only
#   COALESCE(PERFGASFROM,  PROSGASFROM,  PLANGASFROM)  AS gas_from    -- LNG only
#   COALESCE(PERFGASTO,    PROSGASTO,    PLANGASTO)    AS gas_to      -- LNG only
#   COALESCE(PERFWF,       PROSWF,       PLANWF)       AS wf
#   COALESCE(PERFDL,       PROSDL,       PLANDL)       AS dl
#   FROM pjtevemt WHERE PJT = %(shipnum)s
# ※ prefix: PLAN/PROS/PERF  (전망=PROS, 실적=PERF)
# ※ 단일 이벤트: 컬럼명 그대로 (예: PLANLC)
# ※ 범위 이벤트: FROM/TO 접미 (예: PLANSTFROM, PLANSTTO)
# ※ DT(인도시험) 컬럼 없음
#
# 저장 스키마: project, event_type, event_date(FROM), event_date_end(TO), date_type
# 범위 이벤트(A/C, S/T, Cold/T, LNG/B, GAS/T): event_date_end 채움
# 단일 이벤트: event_date_end 공백
#
# TBM 스크립트 표기:
#   단일: "2026-01-20 (D+5일) G/T 발전기 시험"
#   범위: "2026-01-20 ~ 2026-02-10 [진행중] S/T 항해시험"

RANGE_EVENTS   = {"A/C", "S/T", "Cold/T", "LNG/B", "GAS/T"}
RANGE_DURATION = {"A/C": 7, "S/T": 21, "Cold/T": 14, "LNG/B": 7, "GAS/T": 14}

STD_EVENTS = ["L/C","S/P On","B/T","G/T","CMR","M/T","I/E","A/C","S/T","LNG/B","W/F","D/L"]
LNG_EVENTS = ["L/C","S/P On","B/T","G/T","CMR","M/T","I/E","A/C","S/T","Cold/T","LNG/B","GAS/T","W/F","D/L"]
# L/C 기준 누적 경과일
# ※ LNG/B(LNG Bunkering)는 LNG선 전용 아님 — STD/LNG 모두 적용
STD_CUMDAYS = [0, 200, 270, 285, 315, 330, 360, 375, 405, 419,      428, 445]
LNG_CUMDAYS = [0, 200, 270, 285, 315, 330, 360, 375, 405, 420, 435, 442, 458, 475]


def make_milestone_rows(project: str, lc_date: date, is_lng: bool) -> list:
    events  = LNG_EVENTS  if is_lng else STD_EVENTS
    cumdays = LNG_CUMDAYS if is_lng else STD_CUMDAYS
    rows = []
    for i, et in enumerate(events):
        d = lc_date + timedelta(days=cumdays[i])
        is_range = et in RANGE_EVENTS
        end_d = d + timedelta(days=RANGE_DURATION[et]) if is_range else None
        if d < date(2025, 9, 1):
            date_type = "실적"
        elif d <= date(2026, 3, 12):
            date_type = "전망"
        else:
            date_type = "계획"
        rows.append({
            "project":        project,
            "event_type":     et,
            "event_date":     d.strftime("%Y-%m-%d"),
            "event_date_end": end_d.strftime("%Y-%m-%d") if end_d else "",
            "date_type":      date_type,
        })
    return rows


# L/C 날짜: 주요 이벤트가 2026-01 ~ 2026-03 테스트 기간에 위치하도록 설정
MILESTONE_SPECS = [
    # (project, L/C_date,          is_lng)
    ("SN2657", date(2024, 12, 15), False),  # S/T   ≈ 2026-01-24 ~ 2026-02-14
    ("SN2658", date(2025,  4,  1), False),  # G/T   ≈ 2026-01-11
    ("SN2701", date(2024, 12,  1), True),   # GAS/T ≈ 2026-02-05 ~ 2026-02-19
    ("SN2702", date(2025,  5,  1), True),   # M/T   ≈ 2026-02-24
    ("SN2703", date(2024, 10,  1), True),   # D/L   ≈ 2026-01-19
    ("SN2704", date(2025,  2,  1), False),  # S/T   ≈ 2026-03-11 ~ 2026-04-01
    ("SN2705", date(2025,  6,  1), False),  # G/T   ≈ 2026-03-13
    ("SN2706", date(2025,  1,  1), True),   # Cold/T ≈ 2026-02-25 ~ 2026-03-11
    ("SN2801", date(2025,  7,  1), False),  # B/T   ≈ 2026-02-24
    ("SN2802", date(2025,  3,  1), True),   # S/T   ≈ 2026-04-10
]

milestone_rows: list = []
for _proj, _lc, _is_lng in MILESTONE_SPECS:
    milestone_rows.extend(make_milestone_rows(_proj, _lc, _is_lng))
milestone_df = pd.DataFrame(milestone_rows)


# ── pjtevemt 원본 형태 생성 (MySQL pjtevemt wide format) ──────────────────────
# 실제 테이블과 동일한 wide 구조: 1행 = 1호선, 컬럼 = PLAN/PROS/PERF × 이벤트
# COALESCE 우선순위: PERF > PROS > PLAN
EVENT_COL = {
    "L/C":    ("LC",   False),
    "S/P On": ("SP",   False),
    "B/T":    ("BT",   False),
    "G/T":    ("GT",   False),
    "CMR":    ("CMR",  False),
    "M/T":    ("MT",   False),
    "I/E":    ("IE",   False),
    "A/C":    ("AC",   True),
    "S/T":    ("ST",   True),
    "Cold/T": ("COLD", True),
    "LNG/B":  ("LNG",  True),
    "GAS/T":  ("GAS",  True),
    "W/F":    ("WF",   False),
    "D/L":    ("DL",   False),
}

def make_pjtevemt_row(project: str, lc_date: date, is_lng: bool) -> dict:
    events  = LNG_EVENTS  if is_lng else STD_EVENTS
    cumdays = LNG_CUMDAYS if is_lng else STD_CUMDAYS
    row: dict = {"PJT": project}
    for i, et in enumerate(events):
        d = lc_date + timedelta(days=cumdays[i])
        is_range = et in RANGE_EVENTS
        end_d = d + timedelta(days=RANGE_DURATION[et]) if is_range else None
        col_key, _ = EVENT_COL[et]
        d_str   = d.strftime("%Y-%m-%d")
        end_str = end_d.strftime("%Y-%m-%d") if end_d else None
        if d < date(2025, 9, 1):
            dt = "실적"
        elif d <= date(2026, 3, 12):
            dt = "전망"
        else:
            dt = "계획"
        if is_range:
            row[f"PLAN{col_key}FROM"] = d_str
            row[f"PLAN{col_key}TO"]   = end_str
            row[f"PROS{col_key}FROM"] = d_str   if dt in ("전망", "실적") else None
            row[f"PROS{col_key}TO"]   = end_str if dt in ("전망", "실적") else None
            row[f"PERF{col_key}FROM"] = d_str   if dt == "실적" else None
            row[f"PERF{col_key}TO"]   = end_str if dt == "실적" else None
        else:
            row[f"PLAN{col_key}"] = d_str
            row[f"PROS{col_key}"] = d_str if dt in ("전망", "실적") else None
            row[f"PERF{col_key}"] = d_str if dt == "실적" else None
    # LNG 전용 컬럼(Cold/T, GAS/T)은 비LNG선에서 None
    if not is_lng:
        for ck in ["COLD", "GAS"]:
            for pfx in ["PLAN", "PROS", "PERF"]:
                row[f"{pfx}{ck}FROM"] = None
                row[f"{pfx}{ck}TO"]   = None
    return row

pjtevemt_rows = [make_pjtevemt_row(p, lc, lng) for p, lc, lng in MILESTONE_SPECS]
pjtevemt_df = pd.DataFrame(pjtevemt_rows)


# ── Google Drive 업로드 ────────────────────────────────────────────────────────
def get_drive_service():
    creds = Credentials.from_service_account_file(
        KEY_PATH, scopes=["https://www.googleapis.com/auth/drive"]
    )
    return build("drive", "v3", credentials=creds)


def fetch_weather(start: str = "2026-01-01", end: str = "2026-03-11") -> pd.DataFrame:
    """
    Open-Meteo archive API로 실제 날씨 데이터 취득 (울산 기준).
    https://archive-api.open-meteo.com
    - temp_max  : 최고기온 (°C)
    - temp_min  : 최저기온 (°C)
    - rainfall  : 강수량 (mm)
    - wind_speed: 최대풍속 (m/s, API km/h → /3.6 변환)
    - day_of_week: 월~일
    - day_type   : 공휴일/토요일/일요일/평일
    """
    # 울산 좌표 (현대중공업 기준)
    LAT, LON = 35.538, 129.311
    url = (
        f"https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={LAT}&longitude={LON}"
        f"&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max"
        f"&start_date={start}&end_date={end}"
        f"&timezone=Asia%2FSeoul"
    )
    with urllib.request.urlopen(url, timeout=15) as resp:
        data = json.loads(resp.read())

    daily = data["daily"]
    rows = []
    # 2026년 한국 공휴일 (1/1~3/11 범위)
    HOLIDAYS = {
        "2026-01-01",               # 신정
        "2026-02-16",               # 설날 전날
        "2026-02-17",               # 설날
        "2026-02-18",               # 설날 다음날
        "2026-03-01",               # 삼일절 (일요일)
        "2026-03-02",               # 삼일절 대체공휴일
    }
    DOW_KR = ["월", "화", "수", "목", "금", "토", "일"]

    for i, d in enumerate(daily["time"]):
        dt = datetime.strptime(d, "%Y-%m-%d")
        dow = DOW_KR[dt.weekday()]
        if d in HOLIDAYS:
            day_type = "공휴일"
        elif dt.weekday() == 5:
            day_type = "토요일"
        elif dt.weekday() == 6:
            day_type = "일요일"
        else:
            day_type = "평일"

        wind_kmh = daily["wind_speed_10m_max"][i] or 0.0
        rows.append({
            "date":        d,
            "day_of_week": dow,
            "day_type":    day_type,
            "temp_max":    daily["temperature_2m_max"][i],
            "temp_min":    daily["temperature_2m_min"][i],
            "rainfall":    daily["precipitation_sum"][i] or 0.0,
            "wind_speed":  round(wind_kmh / 3.6, 1),
        })
    return pd.DataFrame(rows)


def upload_csv(svc, df: pd.DataFrame, filename: str, folder_id: str):
    res = svc.files().list(
        q=f"name='{filename}' and '{folder_id}' in parents and trashed=false",
        fields="files(id)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True
    ).execute()
    for f in res.get("files", []):
        svc.files().delete(fileId=f["id"], supportsAllDrives=True).execute()

    buf = io.BytesIO(df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig"))
    svc.files().create(
        body={"name": filename, "parents": [folder_id]},
        media_body=MediaIoBaseUpload(buf, mimetype="text/csv"),
        fields="id",
        supportsAllDrives=True
    ).execute()
    print(f"[OK] {filename}  ({len(df)}행)")


# ── 로컬 CSV 저장 (Drive 업로드 전 백업) ──────────────────────────────────────
out_dir = Path(__file__).parent / "output"
out_dir.mkdir(exist_ok=True)
weather_df = fetch_weather("2026-01-01", "2026-03-11")
general_df.to_csv(out_dir / "ptw_general.csv",          index=False, encoding="utf-8-sig")
zlng_df.to_csv(out_dir / "ptw_zlng.csv",               index=False, encoding="utf-8-sig")
ptwlist_df.to_csv(out_dir / "ptwlist.csv",             index=False, encoding="utf-8-sig")
pjtlist_df.to_csv(out_dir / "pjtlist.csv",             index=False, encoding="utf-8-sig")
shipinfo_df.to_csv(out_dir / "shipinfo.csv",           index=False, encoding="utf-8-sig")
pjtevemt_df.to_csv(out_dir / "pjtevent.csv",           index=False, encoding="utf-8-sig")
milestone_df.to_csv(out_dir / "project_milestone.csv", index=False, encoding="utf-8-sig")
weather_df.to_csv(out_dir / "weather.csv",             index=False, encoding="utf-8-sig")
print("[OK] 로컬 저장 완료: GitHub/tbm3/output/")

# ── Drive 업로드 ───────────────────────────────────────────────────────────────
try:
    svc = get_drive_service()
    # MSSQL 서버 데이터 → sample/mssql
    upload_csv(svc, general_df,   "ptw_general.csv",        MSSQL_FOLDER_ID)
    upload_csv(svc, zlng_df,      "ptw_zlng.csv",           MSSQL_FOLDER_ID)
    upload_csv(svc, ptwlist_df,   "ptwlist.csv",            MSSQL_FOLDER_ID)
    # MySQL 원본 → sample/mysql
    upload_csv(svc, shipinfo_df,  "shipinfo.csv",           MYSQL_FOLDER_ID)
    upload_csv(svc, pjtevemt_df,  "pjtevent.csv",           MYSQL_FOLDER_ID)
    # MySQL 가공 + 앱용 파생 → sample/mssql
    upload_csv(svc, pjtlist_df,   "pjtlist.csv",            MSSQL_FOLDER_ID)
    upload_csv(svc, milestone_df, "project_milestone.csv",  MSSQL_FOLDER_ID)
    upload_csv(svc, weather_df,   "weather.csv",             MSSQL_FOLDER_ID)
    print(f"\n[OK] Drive 업로드 완료!")
except Exception as e:
    print(f"\n[WARN] Drive 업로드 실패: {e}")
    print("   로컬 output/ 폴더에서 수동으로 업로드해주세요.")

print(f"\n  ptw_general      : {len(general_df)}행 ({len(working_days)}일 평일)")
print(f"  ptw_zlng         : {len(zlng_df)}행 ({len(working_days)}일 평일)")
print(f"  ptwlist          : {len(ptwlist_df)}행 (병합)")
print(f"  pjtlist          : {len(pjtlist_df)}행")
print(f"  shipinfo         : {len(shipinfo_df)}행")
print(f"  pjtevemt         : {len(pjtevemt_df)}행")
print(f"  project_milestone: {len(milestone_df)}행")
print(f"  weather          : {len(weather_df)}행 (2026-01-01~03-11, 울산 실측)")
