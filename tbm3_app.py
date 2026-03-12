import smtplib
import sys
from datetime import date, datetime
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent))
import db

# ── 페이지 설정 ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="TBM 스크립트 생성기 v2",
    page_icon="🦺",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── 공통 CSS ──────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@300;400;500;700&display=swap');
html, body, [class*="css"] { font-family: 'Noto Sans KR', sans-serif; }
.stApp { background: #f8f9fb; }
[data-testid="stSidebar"] { background: #1e2a3a; }
[data-testid="stSidebar"] * { color: #cdd6e0 !important; }
[data-testid="stSidebar"] .stSelectbox label,
[data-testid="stSidebar"] .stTextInput label,
[data-testid="stSidebar"] .stDateInput label { color: #8fa8c0 !important; font-size:0.78rem !important; }
.script-box { background:white; border-radius:12px; padding:2rem 2.5rem;
              box-shadow:0 2px 12px rgba(0,0,0,0.07); line-height:2; }
.section-title { font-weight:700; font-size:1rem; color:#1e2a3a;
                 border-left:4px solid #2563eb; padding-left:0.7rem;
                 margin:1.5rem 0 0.8rem 0; }
.section-title-lng { font-weight:700; font-size:1rem; color:#1e2a3a;
                     border-left:4px solid #7c3aed; padding-left:0.7rem;
                     margin:1.5rem 0 0.8rem 0; }
.permit-card { background:#f0f4ff; border:1px solid #c7d7ff; border-radius:8px;
               padding:0.8rem 1.2rem; margin-bottom:0.5rem; font-size:0.88rem; }
.lng-card    { background:#faf5ff; border:1px solid #d8b4fe; border-radius:8px;
               padding:0.8rem 1.2rem; margin-bottom:0.5rem; font-size:0.88rem; }
.accident-box { background:#fff7ed; border-left:3px solid #f97316;
                border-radius:0 8px 8px 0; padding:0.8rem 1rem;
                margin-bottom:0.8rem; font-size:0.87rem; }
.weather-badge { display:inline-block; background:#fef3c7; border:1px solid #fcd34d;
                 border-radius:20px; padding:0.2rem 0.8rem; font-size:0.78rem;
                 margin:0.2rem; color:#92400e; }
.risk-badge    { display:inline-block; background:#fee2e2; border:1px solid #fca5a5;
                 border-radius:20px; padding:0.2rem 0.8rem; font-size:0.78rem;
                 margin:0.2rem; color:#991b1b; }
.lng-badge     { display:inline-block; background:#faf5ff; border:1px solid #d8b4fe;
                 border-radius:20px; padding:0.2rem 0.8rem; font-size:0.78rem;
                 margin:0.2rem; color:#6b21a8; }
.project-badge { display:inline-block; background:#1e2a3a; color:white;
                 border-radius:4px; padding:2px 8px; font-size:0.78rem; margin-right:4px; }
.milestone-card { background:#fefce8; border:1px solid #fde047; border-radius:8px;
                  padding:0.6rem 1rem; margin-bottom:0.4rem; font-size:0.85rem; }
.narrator       { color:#6b7280; font-size:0.85rem; font-style:italic; }
.speaker-반장   { color:#1d4ed8; font-weight:600; }
.speaker-반원   { color:#059669; font-weight:600; }
.ref-box { background:#f0fdf4; border:1px solid #86efac; border-radius:8px;
           padding:0.6rem 1rem; margin-top:0.5rem; font-size:0.85rem; }

/* 인쇄 CSS */
@media print {
    [data-testid="stSidebar"],
    .stButton, .stDownloadButton,
    [data-testid="stToolbar"],
    .no-print { display: none !important; }
    .script-box { box-shadow: none; padding: 0; }
    body { font-size: 12pt; }
}
</style>
""", unsafe_allow_html=True)

# ── 상수 ──────────────────────────────────────────────────────────────────────
RISK_MAP = {
    "고압작업":   ["작업 전 차단밸브 잠금 상태 확인 필수", "안면보호구·내압장갑 착용", "작업 반경 내 비인가자 접근 금지"],
    "밀폐작업":   ["진입 전 산소농도·유해가스 측정 필수", "환기 실시 후 입장", "공기호흡기 대기 상태 유지"],
    "시운전작업": ["시운전 전 배관·밸브 연결 상태 최종 확인", "위험 구역 출입 통제", "이상 발생 시 즉시 중단 후 보고"],
    "발전기가동": ["가동 전 연료·냉각수 누설 점검", "가동 중 회전체 접근 금지", "이상음 발생 시 즉시 중단"],
    "전기작업":   ["전원 차단 후 LOTO 적용 필수", "절연 장갑·안전화 착용", "활선 작업 시 별도 허가서 필요"],
    "LNG작업":    ["가스 퍼지 완료 확인 후 작업 개시", "가스 감지기 상시 작동 확인", "극저온 방호장갑 착용 의무"],
    "일반작업":   ["작업구역 정리정돈 후 시작", "개인보호구 착용 확인", "작업 전 위험요소 재확인"],
}

LNG_RISKS = [
    "LNG 누출 감지 시 즉시 대피 후 보고",
    "극저온 화상 방지 — 절연 방호장갑 착용",
    "작업 전 가스 감지기 작동 상태 확인",
    "IGS 계통 및 가스 퍼지 완료 여부 확인",
]

SLOGAN_MAP = {
    "고압작업":   "차단밸브 잠금, 내가 먼저 확인!",
    "밀폐작업":   "가스 측정, 환기 먼저, 그 다음 진입!",
    "시운전작업": "시운전 전 최종 점검, 나부터 시작!",
    "발전기가동": "가동 전 점검 완료, 안전이 우선!",
    "전기작업":   "전원 차단, LOTO 적용, 그 다음 작업!",
    "LNG작업":    "가스 퍼지 완료, 감지기 확인, 안전 진입!",
    "일반작업":   "보호구 착용, 정리정돈, 안전 작업!",
}

EVENT_KR = {
    "L/C":    "용골거치",
    "S/P On": "상가",
    "B/T":    "보일러 시험",
    "G/T":    "발전기 시험",
    "CMT":    "항해의장완료",
    "M/T":    "주기관 시험",
    "I/E":    "경사시험",
    "AC":     "투묘시험",
    "S/T":    "항해시험",
    "Cold/T": "냉각시험",
    "Gas/T":  "가스시험",
    "DT":     "인도시험",
    "W/F":    "인수도",
    "D/L":    "인도",
}


# ── 사이드바 네비게이션 ────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🦺 TBM 시스템 v2")
    st.markdown("---")
    menu = st.radio(
        "메뉴",
        ["📋 TBM 스크립트 생성", "📢 팀장전달사항 관리", "🔴 사고사례 조회"],
        label_visibility="collapsed",
    )
    st.markdown("---")
    st.markdown(
        '<div style="font-size:0.65rem;color:#4a5568;text-transform:uppercase;'
        'letter-spacing:0.1em">TBM Script Generator v2.0</div>',
        unsafe_allow_html=True,
    )


# ══════════════════════════════════════════════════════════════════════════════
# ── 공통 캐시 함수 (Drive 호출 세션 1회로 제한) ──────────────────────────────
@st.cache_data(ttl=300)
def _cached_teams():
    return db.get_all_teams()

@st.cache_data(ttl=300)
def _cached_departments():
    return db.get_all_departments()


# ══════════════════════════════════════════════════════════════════════════════
# 메뉴 1: TBM 스크립트 생성
# ══════════════════════════════════════════════════════════════════════════════
if menu == "📋 TBM 스크립트 생성":

    # ── 사이드바 입력 폼 ───────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown("##### 작업 정보 입력")
        input_date = st.date_input(
            "날짜",
            value=date(2026, 1, 5),
            min_value=date(2026, 1, 1),
            max_value=date(2026, 3, 11),
        )
        input_project = st.text_input("호선번호", placeholder="예: SN2657").strip().upper()

        team_list  = _cached_teams()
        team_opts  = ["(전체)"] + team_list if team_list else ["(전체)"]
        team_sel   = st.selectbox("팀", team_opts)
        input_team = None if team_sel == "(전체)" else team_sel

        dept_list  = _cached_departments()
        dept_opts  = ["(전체)"] + dept_list if dept_list else ["(전체)"]
        dept_sel   = st.selectbox("부서", dept_opts)
        input_dept = None if dept_sel == "(전체)" else dept_sel

        input_name   = st.text_input("이름(manager)", placeholder="예: 김철수").strip()
        generate_btn = st.button("📋 TBM 스크립트 생성", use_container_width=True, type="primary")

    # ── 메인 화면 ──────────────────────────────────────────────────────────────
    st.markdown("# 🦺 TBM 스크립트 생성기")
    st.markdown("날짜·호선·부서·이름을 입력하면 오늘의 TBM 스크립트를 자동 생성합니다.")
    st.markdown("---")

    # ── 공통 렌더 헬퍼 ────────────────────────────────────────────────────────
    def render_health(leader_name, leader_role, section_cls="section-title"):
        st.markdown(f'<div class="{section_cls}">① 건강상태 확인</div>', unsafe_allow_html=True)
        st.markdown(f"""
<div style="padding:0 0.5rem;">
<span class="speaker-반장">{leader_name} {leader_role}:</span>
"자, 다들 모였죠? TBM 시작하겠습니다. 먼저 건강상태 확인합니다.
어젯밤 음주하셨거나 몸 안 좋으신 분 있으면 지금 말씀해주세요."
<br><br>
<span class="narrator">*(팀원 상태 확인)*</span>
<br><br>
<span class="speaker-반장">{leader_name} {leader_role}:</span>
"이상 없죠? 작업 중 이상 생기면 즉시 저한테 얘기해주세요."
</div>""", unsafe_allow_html=True)

    def render_weather(weather, leader_name, leader_role, section_cls="section-title"):
        risks = []
        if weather:
            t_max = weather.get("temp_max", 0)
            t_min = weather.get("temp_min", 0)
            r     = weather.get("rainfall", 0)
            w     = weather.get("wind_speed", 0)
            if t_min <= 0:  risks.append(f"최저기온 {t_min}°C — 블랙아이스·동파 주의")
            if r >= 30:     risks.append(f"강수량 {r}mm — 침수·미끄럼 주의")
            if w >= 15:     risks.append(f"풍속 {w}m/s — 강풍, 외부 고소작업 제한")
            if t_max >= 30: risks.append(f"최고기온 {t_max}°C — 혹서기 온열질환 주의")
            if not risks:   risks.append("특이 기상 위험요소 없음")
        st.markdown(f'<div class="{section_cls}">② 날씨 위험요소</div>', unsafe_allow_html=True)
        if weather:
            badges = (f'<span class="weather-badge">🌡 최고 {weather.get("temp_max","?")}°C</span>'
                      f'<span class="weather-badge">🌡 최저 {weather.get("temp_min","?")}°C</span>'
                      f'<span class="weather-badge">💧 {weather.get("rainfall","?")}mm</span>'
                      f'<span class="weather-badge">💨 {weather.get("wind_speed","?")}m/s</span>')
            st.markdown(f'<div style="margin-bottom:0.8rem;">{badges}</div>', unsafe_allow_html=True)
        st.markdown(f"""
<div style="padding:0 0.5rem;">
<span class="speaker-반장">{leader_name} {leader_role}:</span>
"오늘 날씨 확인했습니다. {'  '.join(risks)}."
<br><br>"이동 시 절대 뛰지 마시고, 핸드레일 잡으면서 다니세요."
</div>""", unsafe_allow_html=True)

    def render_message(msg_dict, leader_name, leader_role, section_cls="section-title"):
        st.markdown(f'<div class="{section_cls}">③ 팀장 전달사항</div>', unsafe_allow_html=True)
        content  = msg_dict.get("content", "") if msg_dict else ""
        ref_type = msg_dict.get("ref_type", "") if msg_dict else ""
        ref_path = msg_dict.get("ref_path", "") if msg_dict else ""
        if content:
            st.markdown(f"""
<div style="padding:0 0.5rem;">
<span class="speaker-반장">{leader_name} {leader_role}:</span>
"팀장님 전달사항 전달합니다."
<br><br>
<div style="background:#eff6ff; border-left:3px solid #3b82f6;
     padding:0.8rem 1rem; border-radius:0 8px 8px 0; margin:0.5rem 0;">
📢 <strong>"{content}"</strong>
</div>
</div>""", unsafe_allow_html=True)
            # 참고자료 표시
            if ref_type and ref_path and ref_path not in ("", "nan"):
                st.markdown('<div class="ref-box no-print">', unsafe_allow_html=True)
                if ref_type == "link":
                    st.markdown(f'📎 참고자료: <a href="{ref_path}" target="_blank">{ref_path}</a>',
                                unsafe_allow_html=True)
                elif ref_type == "pdf":
                    st.markdown(f'📄 <a href="{ref_path}" target="_blank">참고 PDF 다운로드</a>',
                                unsafe_allow_html=True)
                elif ref_type == "image":
                    try:
                        st.image(ref_path, width=300)
                    except Exception:
                        st.markdown(f'🖼 참고 이미지: {ref_path}', unsafe_allow_html=True)
                elif ref_type == "video":
                    st.markdown(f'🎬 참고 동영상: <a href="{ref_path}" target="_blank">{ref_path}</a>',
                                unsafe_allow_html=True)
                st.markdown('</div>', unsafe_allow_html=True)
        else:
            st.markdown(f"""
<div style="padding:0 0.5rem;">
<span class="speaker-반장">{leader_name} {leader_role}:</span>
"오늘 별도 팀장 전달사항 없습니다. 기본 안전수칙 철저히 지켜주세요."
</div>""", unsafe_allow_html=True)

    def render_milestones(milestones, date_str, leader_name, leader_role,
                          section_cls="section-title"):
        st.markdown(f'<div class="{section_cls}">⑤ 호선 주요 일정</div>', unsafe_allow_html=True)
        if milestones is not None and not milestones.empty:
            base = datetime.strptime(date_str, "%Y-%m-%d").date()
            DT_COLOR = {"실적": "#059669", "전망": "#2563eb", "계획": "#9ca3af"}
            first_mention = ""
            for _, ms in milestones.iterrows():
                end_str = str(ms.get("event_date_end", "") or "").strip()
                delta   = (ms["event_date"] - base).days
                en      = EVENT_KR.get(ms["event_type"], ms["event_type"])
                dt_lbl  = str(ms.get("date_type", ""))
                dt_col  = DT_COLOR.get(dt_lbl, "#9ca3af")

                if end_str:
                    # 범위 이벤트 (S/T, Cold/T, Gas/T 등)
                    end_date = datetime.strptime(end_str, "%Y-%m-%d").date()
                    end_delta = (end_date - base).days
                    if delta <= 0 <= end_delta:
                        timing = "진행중"
                        timing_color = "#dc2626"
                    elif delta > 0:
                        timing = f"D+{delta}일 시작"
                        timing_color = "#2563eb"
                    else:
                        timing = f"완료 ({abs(end_delta)}일 전)"
                        timing_color = "#6b7280"
                    date_display = f"{ms['event_date']} ~ {end_str}"
                    if not first_mention:
                        first_mention = f"{'현재' if delta<=0<=end_delta else '곧'} {en} 진행 중입니다."
                else:
                    # 단일 이벤트
                    timing = ("오늘" if delta == 0 else
                              f"D+{delta}일" if delta > 0 else f"D{delta}일(경과)")
                    timing_color = "#dc2626" if delta == 0 else ("#2563eb" if delta > 0 else "#6b7280")
                    date_display = str(ms["event_date"])
                    if not first_mention and delta >= 0:
                        first_mention = (f"오늘 {en}이 있습니다." if delta == 0
                                         else f"앞으로 {delta}일 후 {en}이 예정되어 있습니다.")

                st.markdown(f"""
<div class="milestone-card">
📅 <strong>{date_display}</strong> &nbsp;
<span style="color:{timing_color};font-weight:600;">({timing})</span> &nbsp;
<span style="background:#1e2a3a;color:white;border-radius:4px;padding:1px 8px;font-size:0.78rem;">{ms['event_type']}</span>
&nbsp; {en} &nbsp;
<span style="background:{dt_col};color:white;border-radius:4px;padding:1px 6px;font-size:0.72rem;">{dt_lbl}</span>
</div>""", unsafe_allow_html=True)

            mention = first_mention or f"{EVENT_KR.get(milestones.iloc[0]['event_type'],'')} 일정을 확인하세요."
            st.markdown(f"""
<div style="padding:0 0.5rem; margin-bottom:1rem;">
<span class="speaker-반장">{leader_name} {leader_role}:</span>
"{mention} 검사 전후 안전 관리에 더욱 신경 써주세요."
</div>""", unsafe_allow_html=True)
        else:
            st.markdown('<div style="color:#6b7280;font-size:0.88rem;padding:0 0.5rem;">'
                        '향후 14일 내 주요 마일스톤 없음</div>', unsafe_allow_html=True)

    def render_accidents(accidents, leader_name, leader_role, section_cls="section-title"):
        st.markdown(f'<div class="{section_cls}">⑥ 유사 사고 사례</div>', unsafe_allow_html=True)
        if accidents is not None and not accidents.empty:
            for i, (_, acc) in enumerate(accidents.iterrows(), 1):
                pdf_url = None
                if pd.notna(acc.get("pdf_filename", "")) and acc.get("pdf_filename", ""):
                    pdf_url = db.get_accident_pdf_url(acc["pdf_filename"])
                st.markdown(f"""
<div class="accident-box">
  <div style="font-weight:600; margin-bottom:0.4rem;">
    🔴 사고 {i} &nbsp;[{acc.get('accident_type','')}] &nbsp;
    <span style="font-weight:400; color:#9a3412;">{acc.get('date','')}</span>
  </div>
  <div><strong>개요:</strong> {acc.get('summary','')}</div>
  <div style="margin-top:0.3rem;"><strong>원인:</strong> {acc.get('cause','')}</div>
  <div style="margin-top:0.3rem;color:#b91c1c;"><strong>피해:</strong> {acc.get('result','')}</div>
  <div style="margin-top:0.3rem;"><strong>대책:</strong> {acc.get('countermeasure','')}</div>
  <div style="margin-top:0.3rem;font-size:0.78rem;color:#9ca3af;">출처: {acc.get('source','')}</div>
</div>""", unsafe_allow_html=True)
                if pdf_url:
                    st.markdown(
                        f'<a href="{pdf_url}" target="_blank" class="no-print">'
                        f'📄 사고사례 원본 PDF 다운로드</a>',
                        unsafe_allow_html=True,
                    )
            st.markdown(f"""
<div style="padding:0 0.5rem; margin-bottom:1rem;">
<span class="speaker-반장">{leader_name} {leader_role}:</span>
"오늘 작업과 유사한 사고입니다. 남의 일이 아닙니다. 반드시 명심하세요."
</div>""", unsafe_allow_html=True)

    def render_slogan(slogan, leader_name, leader_role, section_cls="section-title"):
        st.markdown(f'<div class="{section_cls}">⑦ 안전구호</div>', unsafe_allow_html=True)
        st.markdown(f"""
<div style="padding:0 0.5rem; margin-bottom:0.8rem;">
<span class="speaker-반장">{leader_name} {leader_role}:</span>
"오늘 안전구호는 <strong>'{slogan}'</strong> 로 하겠습니다. 안전구호 제창!"
<br><br>
<span class="speaker-반원">반원 일동:</span>
<strong>"{slogan} 좋아 좋아 좋아!"</strong>
<br><br>
<span class="speaker-반장">{leader_name} {leader_role}:</span>
"자, 다들 오늘도 안전하게 마무리합시다. 수고하십시오!"
</div>
</div>""", unsafe_allow_html=True)

    # ── 인쇄 / 이메일 유틸 ────────────────────────────────────────────────────
    def render_print_button():
        st.markdown("""
<div class="no-print" style="margin-bottom:1rem;">
<script>function printScript(){window.print();}</script>
</div>""", unsafe_allow_html=True)
        if st.button("🖨️ 인쇄", key="print_btn"):
            st.markdown(
                "<script>window.print();</script>",
                unsafe_allow_html=True,
            )

    def render_email_form(subject_default: str, html_body: str,
                          accident_df: pd.DataFrame = None):
        smtp_cfg = st.secrets.get("smtp", {})
        with st.expander("📧 이메일 전송", expanded=False):
            col1, col2 = st.columns(2)
            with col1:
                to_input = st.text_input("수신자 (쉼표 구분)", key="email_to",
                                         placeholder="a@co.com, b@co.com")
            with col2:
                cc_input = st.text_input("참조(CC)", key="email_cc",
                                         placeholder="선택 사항")
            subject = st.text_input("제목", value=subject_default, key="email_subject")
            attach_pdf = False
            if accident_df is not None and not accident_df.empty:
                attach_pdf = st.checkbox("사고사례 PDF 첨부", value=False, key="email_attach")
            if st.button("전송", key="email_send", type="primary"):
                if not to_input.strip():
                    st.error("수신자를 입력해주세요.")
                elif not smtp_cfg.get("server"):
                    st.error("SMTP 설정이 없습니다. .streamlit/secrets.toml 을 확인해주세요.")
                else:
                    try:
                        to_list = [e.strip() for e in to_input.split(",") if e.strip()]
                        cc_list = [e.strip() for e in cc_input.split(",") if e.strip()]
                        msg = MIMEMultipart("mixed")
                        msg["From"]    = f"{smtp_cfg.get('from_name','TBM')} <{smtp_cfg['user']}>"
                        msg["To"]      = ", ".join(to_list)
                        if cc_list:
                            msg["Cc"]  = ", ".join(cc_list)
                        msg["Subject"] = subject
                        msg.attach(MIMEText(html_body, "html", "utf-8"))

                        if attach_pdf and accident_df is not None:
                            for _, row in accident_df.iterrows():
                                pdf_fn = row.get("pdf_filename", "")
                                if not pdf_fn:
                                    continue
                                pdf_bytes = db.get_accident_pdf_bytes(pdf_fn)
                                if pdf_bytes:
                                    part = MIMEBase("application", "pdf")
                                    part.set_payload(pdf_bytes)
                                    encoders.encode_base64(part)
                                    part.add_header("Content-Disposition",
                                                    f'attachment; filename="{pdf_fn}"')
                                    msg.attach(part)

                        all_to = to_list + cc_list
                        with smtplib.SMTP(smtp_cfg["server"], int(smtp_cfg.get("port", 587))) as s:
                            s.ehlo()
                            s.starttls()
                            s.login(smtp_cfg["user"], smtp_cfg["password"])
                            s.sendmail(smtp_cfg["user"], all_to, msg.as_string())
                        st.success(f"전송 완료: {', '.join(to_list)}")
                    except Exception as e:
                        st.error(f"전송 실패: {e}")

    # ── 스크립트 생성 로직 ────────────────────────────────────────────────────
    def build_script(target_date, project, team, department, name):
        date_str  = target_date.strftime("%Y-%m-%d")
        date_kor  = target_date.strftime("%Y년 %m월 %d일")
        dow       = ["월","화","수","목","금","토","일"][target_date.weekday()]

        # 1순위: project + team + department / 2순위: manager(name)
        ptwlist    = db.get_ptwlist(date_str, project, team, department, name)
        daily_work = pd.DataFrame()
        if ptwlist.empty:
            all_dw = db.get_daily_work()
            if not all_dw.empty:
                daily_work = all_dw.sample(min(2, len(all_dw)), random_state=None)

        weather   = db.get_weather(date_str)
        eff_team  = team or (ptwlist.iloc[0]["team"] if not ptwlist.empty else "")
        msg_dict  = db.get_message(date_str, eff_team) if eff_team else {}
        milestones = db.get_milestones(project, date_str, 14) if project else None

        # 사고사례: PTW work_type 또는 daily_work work_category 기준
        if not ptwlist.empty:
            work_types = ptwlist["work_type"].dropna().unique().tolist()
            accidents  = db.get_accident_cases(work_types[0], 2) if work_types else pd.DataFrame()
        elif not daily_work.empty:
            kw = daily_work.iloc[0]["work_category"]
            accidents = db.get_accident_cases(kw, 2)
        else:
            accidents = pd.DataFrame()

        leader_name = name if name else (
            ptwlist.iloc[0]["manager"] if not ptwlist.empty else
            (daily_work.iloc[0]["team"] if not daily_work.empty else "반장")
        )
        leader_role = ""
        proj_info   = db.get_project_info(project) if project else {}

        # ── 인쇄/이메일 버튼 ──────────────────────────────────────────────
        top_col1, top_col2 = st.columns([1, 5])
        with top_col1:
            render_print_button()

        # 헤더
        type_badge = ""
        if proj_info:
            type_badge = f'<span class="project-badge">{proj_info.get("TYPEMODEL","")}</span>'
            if proj_info.get("FUEL") == "DF":
                type_badge += f'<span class="project-badge" style="background:#7c3aed;">DF</span>'

        st.markdown(f"""
<div class="script-box">
<div style="text-align:center; margin-bottom:1.5rem;">
  <div style="font-size:1.4rem; font-weight:700; color:#1e2a3a;">📋 TBM (Tool Box Meeting)</div>
  <div style="font-size:0.9rem; color:#6b7280; margin-top:0.3rem;">
    {date_kor}({dow}) &nbsp;|&nbsp; {department or ''} &nbsp;|&nbsp; 호선: {project or ''}
    &nbsp; {type_badge}
  </div>
  <div style="font-size:0.85rem; color:#9ca3af; margin-top:0.3rem;">
    진행: {leader_name} {leader_role}
  </div>
</div>
""", unsafe_allow_html=True)

        render_health(leader_name, leader_role)
        render_weather(weather, leader_name, leader_role)
        render_message(msg_dict, leader_name, leader_role)

        # ④ 작업허가 내용 / 일상작업 폴백
        if not ptwlist.empty:
            st.markdown('<div class="section-title">④ 작업허가 내용 및 위험요소</div>',
                        unsafe_allow_html=True)
        else:
            st.markdown('<div class="section-title">④ 일상작업 위험요소 (PTW 미발행)</div>',
                        unsafe_allow_html=True)
            st.info("해당 날짜에 PTW가 없습니다. 일상작업으로 제공합니다.")
            if daily_work.empty:
                st.warning("일상작업 데이터를 불러올 수 없습니다.")
            else:
                for i, (_, dw) in enumerate(daily_work.iterrows(), 1):
                    risk_hint = str(dw.get("risk_hint", ""))
                    risk_items = "".join([
                        f'<span class="risk-badge">⚠ {r.strip()}</span>'
                        for r in risk_hint.split("·") if r.strip()
                    ])
                    st.markdown(f"""
<div class="permit-card">
  <div style="font-weight:600; margin-bottom:0.4rem;">
    🔧 일상작업 {i} &nbsp;
    <span style="background:#dbeafe;color:#1d4ed8;border-radius:4px;
          padding:1px 6px;font-size:0.78rem;">{dw['work_category']}</span>
  </div>
  <div style="color:#374151;">{dw['work_detail']}</div>
</div>
<div style="padding:0 0.5rem; margin-bottom:1rem;">
<span class="speaker-반장">{leader_name} {leader_role}:</span>
"오늘 <strong>{dw['work_detail']}</strong> 작업이 예상됩니다. 다음 위험요소를 반드시 확인하세요."
<br><br>{risk_items}
</div>""", unsafe_allow_html=True)

        if not ptwlist.empty:
            for _, row in ptwlist.iterrows():
                src = row.get("source", "")
                wt  = row.get("work_type", "일반작업")
                risks = RISK_MAP.get(wt, RISK_MAP["일반작업"])

                if src == "ptw_zlng":
                    # LNG 카드 (보라색)
                    card_cls = "lng-card"
                    badge_bg = "#7c3aed"
                    risk_items = "".join([f'<span class="risk-badge">⚠ {r}</span>' for r in risks])
                    lng_items  = "".join([f'<span class="lng-badge">🔵 {r}</span>' for r in LNG_RISKS])
                    st.markdown(f"""
<div class="{card_cls}">
  <div style="font-weight:600; margin-bottom:0.4rem;">
    🔷 {row['work_detail']} &nbsp;
    <span style="background:{badge_bg};color:white;border-radius:4px;padding:1px 6px;font-size:0.78rem;">{wt}</span>
    <span style="background:#6b21a8;color:white;border-radius:4px;padding:1px 6px;font-size:0.78rem;margin-left:4px;">LNG</span>
  </div>
  <div style="color:#4b5563;font-size:0.85rem;">
    🚢 호선: <strong>{row['project']}</strong> &nbsp;|&nbsp;
    📍 구역: <strong>{row['area']}</strong> &nbsp;|&nbsp;
    👤 신청자: <strong>{row['manager']}</strong>
  </div>
</div>
<div style="padding:0 0.5rem; margin-bottom:1rem;">
<span class="speaker-반장">{leader_name} {leader_role}:</span>
"{row['project']}호선 {row['area']}에서 <strong>{row['work_detail']}</strong> 진행합니다."
<br><br>
<strong>일반 위험요소:</strong><br>{risk_items}
<br><br>
<strong>LNG 특별 주의사항:</strong><br>{lng_items}
</div>""", unsafe_allow_html=True)
                else:
                    # 일반 카드 (파란색)
                    risk_items = "".join([f'<span class="risk-badge">⚠ {r}</span>' for r in risks])
                    st.markdown(f"""
<div class="permit-card">
  <div style="font-weight:600; margin-bottom:0.4rem;">
    📄 {row['work_detail']} &nbsp;
    <span style="background:#dbeafe;color:#1d4ed8;border-radius:4px;padding:1px 6px;font-size:0.78rem;">{wt}</span>
  </div>
  <div style="color:#4b5563;font-size:0.85rem;">
    🚢 호선: <strong>{row['project']}</strong> &nbsp;|&nbsp;
    📍 구역: <strong>{row['area']}</strong> &nbsp;|&nbsp;
    👤 신청자: <strong>{row['manager']}</strong>
  </div>
</div>
<div style="padding:0 0.5rem; margin-bottom:1rem;">
<span class="speaker-반장">{leader_name} {leader_role}:</span>
"{row['project']}호선 {row['area']}에서 <strong>{row['work_detail']}</strong> 진행합니다."
<br><br>{risk_items}
</div>""", unsafe_allow_html=True)

        render_milestones(milestones, date_str, leader_name, leader_role)
        render_accidents(accidents, leader_name, leader_role)

        if not ptwlist.empty:
            main_wt = ptwlist.iloc[0]["work_type"]
        elif not daily_work.empty:
            main_wt = daily_work.iloc[0].get("work_category", "일반작업")
        else:
            main_wt = "일반작업"
        render_slogan(SLOGAN_MAP.get(main_wt, "안전 제일, 무재해 작업!"), leader_name, leader_role)

        # ── 이메일 폼 ──────────────────────────────────────────────────────
        subject_default = f"TBM 스크립트 - {date_str} {department or ''}"
        if not ptwlist.empty:
            work_list_html = ''.join([
                f"<p>• [{r.get('source','')[:3]}] {r['work_detail']} ({r['work_type']}) — {r['project']} {r['area']}</p>"
                for _, r in ptwlist.iterrows()
            ])
        elif not daily_work.empty:
            work_list_html = ''.join([
                f"<p>• [일상] {dw['work_detail']} ({dw['work_category']}) | 위험: {dw['risk_hint']}</p>"
                for _, dw in daily_work.iterrows()
            ])
        else:
            work_list_html = '<p>(없음)</p>'

        html_body = f"""
<h2>TBM 스크립트 - {date_kor}({dow})</h2>
<p><b>팀:</b> {team or ''} &nbsp; <b>부서:</b> {department or ''} &nbsp; <b>호선:</b> {project or ''} &nbsp; <b>진행:</b> {leader_name}</p>
<hr>
<h3>③ 팀장 전달사항</h3>
<p>{msg_dict.get('content','(없음)') if msg_dict else '(없음)'}</p>
<h3>④ 작업 목록</h3>
{work_list_html}
<h3>⑥ 사고사례 요약</h3>
{''.join([f"<p>• [{a.get('accident_type','')}] {a.get('summary','')[:60]}...</p>"
          for _, a in accidents.iterrows()]) if not accidents.empty else '<p>(없음)</p>'}
<hr><p style="color:gray;font-size:0.8em;">TBM Script Generator v2.0</p>
"""
        render_email_form(subject_default, html_body, accidents)

    if generate_btn:
        if not input_project and not input_team and not input_dept and not input_name:
            st.warning("호선번호, 팀, 부서, 이름 중 하나 이상 입력해주세요.")
        else:
            build_script(input_date, input_project or None, input_team,
                         input_dept, input_name or None)
    else:
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown("""
**📌 사용 방법**
1. 날짜·호선·팀·부서 입력 (1순위)
2. 이름 입력 시 manager 컬럼 추가 검색 (2순위)
3. PTW 있으면 → 작업허가서 스크립트
4. PTW 없으면 → 일상작업 스크립트
5. **TBM 스크립트 생성** 클릭 후 인쇄/이메일
""")
        with c2:
            st.markdown("""
**📄 일반 PTW 모드**
- 파란색 카드로 표시
- 작업유형별 위험요소 안내
""")
        with c3:
            st.markdown("""
**🔷 LNG PTW 모드**
- 보라색 카드로 표시
- LNG 특별 위험요소 추가 표시
""")


# ══════════════════════════════════════════════════════════════════════════════
# 메뉴 2: 팀장전달사항 관리
# ══════════════════════════════════════════════════════════════════════════════
elif menu == "📢 팀장전달사항 관리":

    from datetime import timedelta

    with st.sidebar:
        st.markdown("##### 조회 조건")
        teams = db.get_all_teams() or ["A팀", "B팀"]
        msg_team = st.selectbox("팀", teams)

    st.markdown("# 📢 팀장전달사항 관리")
    st.markdown("---")

    # ── 날짜 이동 (세션 상태) ──────────────────────────────────────────────
    if "msg_date" not in st.session_state:
        st.session_state.msg_date = date(2026, 1, 5)

    nav_col1, nav_col2, nav_col3 = st.columns([1, 3, 1])
    with nav_col1:
        if st.button("◀ 전날", use_container_width=True):
            st.session_state.msg_date -= timedelta(days=1)
            st.rerun()
    with nav_col2:
        st.markdown(
            f'<div style="text-align:center; font-size:1.1rem; font-weight:600; padding:0.4rem 0;">'
            f'{st.session_state.msg_date.strftime("%Y년 %m월 %d일")} ({["월","화","수","목","금","토","일"][st.session_state.msg_date.weekday()]})'
            f'</div>',
            unsafe_allow_html=True,
        )
    with nav_col3:
        if st.button("다음날 ▶", use_container_width=True):
            st.session_state.msg_date += timedelta(days=1)
            st.rerun()

    msg_date = st.session_state.msg_date
    msg_date_str = msg_date.strftime("%Y-%m-%d")

    # ── 해당 날짜+팀 기존 데이터 로드 ────────────────────────────────────
    existing = db.get_message(msg_date_str, msg_team)
    existing_content  = existing.get("content", "")  if existing else ""
    existing_ref_type = existing.get("ref_type", "") if existing else ""
    existing_ref_path = existing.get("ref_path", "") if existing else ""
    # "nan" 문자열 정리
    if existing_ref_type in ("nan", "None"): existing_ref_type = ""
    if existing_ref_path in ("nan", "None"): existing_ref_path = ""

    if existing_content:
        st.success(f"**{msg_team}** / {msg_date_str} — 기존 전달사항이 있습니다. 수정 후 저장하세요.")
    else:
        st.info(f"**{msg_team}** / {msg_date_str} — 등록된 전달사항이 없습니다.")

    # ── 등록/수정 폼 ──────────────────────────────────────────────────────
    with st.form("msg_form"):
        new_content = st.text_area(
            "전달사항 내용",
            value=existing_content,
            placeholder="예: 이번 주 전사 안전점검 주간입니다. 작업 전 후 안전점검 철저히 실시하세요.",
            height=120,
        )

        st.markdown("**참고자료 (선택 — 파일·링크 동시 저장 가능)**")
        ref_col1, ref_col2 = st.columns(2)
        with ref_col1:
            uploaded = st.file_uploader(
                "파일 첨부 (PDF/이미지/동영상)",
                type=["pdf", "jpg", "jpeg", "png", "mp4"],
                key="ref_upload",
            )
        with ref_col2:
            link_input = st.text_input(
                "링크 URL",
                value=existing_ref_path if existing_ref_type == "link" else "",
                placeholder="https://...",
            )

        save_col, del_col = st.columns([3, 1])
        with save_col:
            submitted = st.form_submit_button("💾 저장 (덮어쓰기)", type="primary", use_container_width=True)
        with del_col:
            delete_btn = st.form_submit_button("🗑 삭제", use_container_width=True)

    if submitted:
        if not new_content.strip():
            st.error("전달사항 내용을 입력해주세요.")
        else:
            ref_type = existing_ref_type
            ref_path = existing_ref_path
            mime_map = {".pdf":"application/pdf", ".jpg":"image/jpeg",
                        ".jpeg":"image/jpeg", ".png":"image/png", ".mp4":"video/mp4"}
            type_map = {".pdf":"pdf", ".jpg":"image", ".jpeg":"image",
                        ".png":"image", ".mp4":"video"}

            if uploaded is not None:
                ext = Path(uploaded.name).suffix.lower()
                try:
                    file_ref = db.upload_ref_file(
                        uploaded.getvalue(), uploaded.name,
                        mime_map.get(ext, "application/octet-stream")
                    )
                    ref_type = type_map.get(ext, "file")
                    if file_ref and not file_ref.startswith("ref_files/"):
                        ref_path = db.get_drive_download_url(file_ref)
                    else:
                        ref_path = file_ref
                    st.success(f"파일 저장 완료: {uploaded.name}")
                except Exception as e:
                    st.warning(f"파일 저장 실패: {e}")

            if link_input.strip():
                ref_type = "link"
                ref_path = link_input.strip()

            try:
                db.save_message(msg_date_str, msg_team, new_content.strip(), ref_type, ref_path)
                st.success("저장됐습니다.")
                st.rerun()
            except Exception as e:
                st.error(f"저장 실패: {e}")

    if delete_btn:
        if existing_content:
            try:
                db.save_message(msg_date_str, msg_team, "", "", "")
                st.success("삭제됐습니다.")
                st.rerun()
            except Exception as e:
                st.error(f"삭제 실패: {e}")
        else:
            st.warning("삭제할 전달사항이 없습니다.")


# ══════════════════════════════════════════════════════════════════════════════
# 메뉴 3: 사고사례 조회
# ══════════════════════════════════════════════════════════════════════════════
elif menu == "🔴 사고사례 조회":

    with st.sidebar:
        st.markdown("##### 필터")
        work_types_all = ["전체", "LNG작업", "고압작업", "밀폐작업",
                          "시운전작업", "발전기가동", "전기작업", "일반작업"]
        acc_filter = st.selectbox("작업유형", work_types_all)

    st.markdown("# 🔴 사고사례 조회")
    st.markdown("---")

    wt_query = None if acc_filter == "전체" else acc_filter
    acc_df = db.get_all_accidents(wt_query)

    if acc_df.empty:
        st.info("사고사례 데이터가 없습니다. generate_accident_data.py를 먼저 실행하고 Drive에 업로드해주세요.")
        st.markdown("**로컬 accident.csv 파일로 미리보기:**")
        local_csv = Path(__file__).parent / "output" / "accident.csv"
        if local_csv.exists():
            acc_df = pd.read_csv(local_csv, encoding="utf-8-sig")
            if wt_query:
                acc_df = acc_df[acc_df["work_keywords"].str.contains(wt_query, na=False)]
        else:
            st.warning("output/accident.csv 파일도 없습니다.")
            acc_df = pd.DataFrame()

    if not acc_df.empty:
        st.markdown(f"**총 {len(acc_df)}건** {'(전체)' if not wt_query else f'({wt_query})'}")

        for _, row in acc_df.iterrows():
            local_pdf = (Path(__file__).parent / "output" / "accident_pdf"
                         / str(row.get("pdf_filename", "")))
            with st.expander(
                f"[{row.get('accident_type','')}] {row.get('date','')} — {row.get('summary','')[:50]}..."
            ):
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.markdown(f"""
**사고 개요**
{row.get('summary','')}

**사고 원인**
{row.get('cause','')}

**사고 결과/피해**
<span style="color:#b91c1c;">{row.get('result','')}</span>

**재발 방지 대책**
{row.get('countermeasure','')}

*출처: {row.get('source','')}*
""", unsafe_allow_html=True)
                with col2:
                    st.markdown(f"""
| 항목 | 내용 |
|------|------|
| 사고유형 | {row.get('accident_type','')} |
| 관련작업 | {row.get('work_keywords','')} |
| 사고ID | {row.get('id','')} |
""")
                    # PDF 다운로드 (Drive 또는 로컬)
                    pdf_fn = str(row.get("pdf_filename", ""))
                    drive_url = db.get_accident_pdf_url(pdf_fn) if pdf_fn else None
                    if drive_url:
                        st.markdown(
                            f'<a href="{drive_url}" target="_blank">📄 PDF 다운로드 (Drive)</a>',
                            unsafe_allow_html=True,
                        )
                    elif local_pdf.exists():
                        st.download_button(
                            "📄 PDF 다운로드 (로컬)",
                            data=local_pdf.read_bytes(),
                            file_name=pdf_fn,
                            mime="application/pdf",
                            key=f"dl_{row.get('id','')}",
                        )
