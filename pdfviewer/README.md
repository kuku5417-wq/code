# PDF Viewer

Google Drive `sample` 폴더의 PDF 추출 결과를 보는 Streamlit 앱

## 실행 방법

### 1. 패키지 설치
```powershell
pip install -r requirements.txt
```

### 2. Google 인증 설정 (둘 중 하나)

**방법 A - Application Default Credentials (권장)**
```powershell
gcloud auth application-default login
```

**방법 B - 환경변수로 폴더 ID 설정**
```powershell
$env:GOOGLE_DRIVE_SAMPLE_FOLDER_ID = "1iEcz2dXFTQyXtVAINXZzKB9wJfGhyzyU"
```

### 3. 실행
```powershell
streamlit run pdfviewer.py
```

브라우저에서 `http://localhost:8501` 자동으로 열려요!

## 사용법
1. 왼쪽 사이드바에 **SAMPLE 폴더 ID** 입력
   - `1iEcz2dXFTQyXtVAINXZzKB9wJfGhyzyU`
2. 보고 싶은 PDF 선택
3. **텍스트 탭**: 추출된 텍스트 확인
4. **이미지 탭**: 추출된 이미지 확인 및 다운로드
