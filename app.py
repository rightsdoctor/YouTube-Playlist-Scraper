import streamlit as st
import json
import re
import os
import glob
import subprocess
import shutil
import zipfile
import threading
import base64
import pandas as pd
from datetime import datetime
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============================================================
# 페이지 설정
# ============================================================
st.set_page_config(page_title="YT Playlist Scraper", layout="wide")
st.title("YouTube Playlist Scraper")
st.caption("플레이리스트 URL → 메타데이터 + 자막 → Excel / CSV / 자막 파일")

# ============================================================
# 헬퍼 함수
# ============================================================
INTERNAL_FORMAT = "srt"
# ★ 절대 경로 사용 — Streamlit Cloud에서도 안정적
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SUBTITLE_DIR = os.path.join(BASE_DIR, "subtitles_temp")
CONVERTED_DIR = os.path.join(BASE_DIR, "subtitles_converted")


def check_ffmpeg_available() -> bool:
    """ffmpeg 설치 여부 확인"""
    try:
        subprocess.run(["ffmpeg", "-version"],
                       capture_output=True, text=True, timeout=5)
        return True
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def srt_to_plain_text(srt_content: str) -> str:
    lines = srt_content.strip().split('\n')
    text_lines = []
    for line in lines:
        line = line.strip()
        if re.match(r'^\d+$', line):
            continue
        if re.match(r'\d{2}:\d{2}:\d{2}', line):
            continue
        line = re.sub(r'<[^>]+>', '', line)
        if line:
            text_lines.append(line)
    deduplicated = []
    for t in text_lines:
        if not deduplicated or t != deduplicated[-1]:
            deduplicated.append(t)
    return ' '.join(deduplicated)


def format_duration(seconds):
    if not seconds:
        return ''
    seconds = int(seconds)
    h, remainder = divmod(seconds, 3600)
    m, s = divmod(remainder, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


def read_subtitle_files(video_id, subtitle_dir, ext):
    """
    ★ 수정: glob 패턴을 더 유연하게.
    yt-dlp 자막 파일명: {video_id}.{lang}.{ext}
    """
    # 정확한 패턴: video_id.*.ext
    pattern = os.path.join(subtitle_dir, f"{video_id}.*.{ext}")
    sub_files = glob.glob(pattern)

    # fallback: video_id로 시작하는 모든 파일
    if not sub_files:
        pattern = os.path.join(subtitle_dir, f"{video_id}*.{ext}")
        sub_files = glob.glob(pattern)

    result = {}
    for fpath in sub_files:
        fname = os.path.basename(fpath)
        # "VIDEO_ID.ko.srt" → lang = "ko"
        parts = fname.replace(f".{ext}", "").split(".")
        lang = parts[-1] if len(parts) > 1 else "unknown"
        with open(fpath, 'r', encoding='utf-8', errors='replace') as f:
            result[lang] = f.read()
    return result


def zip_directory(dir_path, ext):
    buf = BytesIO()
    matched = glob.glob(os.path.join(dir_path, f"*.{ext}"))
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
        for fpath in matched:
            zf.write(fpath, os.path.basename(fpath))
    return buf.getvalue(), len(matched)


def make_download_link(data: bytes, filename: str, label: str) -> str:
    b64 = base64.b64encode(data).decode()
    return (
        f'<a href="data:application/octet-stream;base64,{b64}" '
        f'download="{filename}" '
        f'style="display:inline-block;padding:0.5rem 1rem;'
        f'background-color:#FF4B4B;color:white;text-decoration:none;'
        f'border-radius:0.5rem;font-weight:600;text-align:center;'
        f'width:100%;box-sizing:border-box;">'
        f'{label}</a>'
    )


# ============================================================
# 사이드바
# ============================================================
with st.sidebar:
    st.header("설정")
    playlist_url = st.text_input(
        "플레이리스트 URL",
        placeholder="https://www.youtube.com/playlist?list=..."
    )

    st.subheader("자막 옵션")
    sub_mode = st.radio(
        "자막 수집 방식",
        ["수동 자막만", "자동 생성 자막만", "수동 우선, 없으면 자동", "수동 + 자동 모두"],
        index=2,
    )
    sub_mode_map = {
        "수동 자막만": "1",
        "자동 생성 자막만": "2",
        "수동 우선, 없으면 자동": "3",
        "수동 + 자동 모두": "4",
    }
    sub_choice = sub_mode_map[sub_mode]
    sub_lang = st.text_input("자막 언어", value="ko",
                             help="예: ko, en, ja 또는 all")
    output_format = st.selectbox("자막 파일 포맷", ["txt", "srt", "vtt", "docx"])
    run_btn = st.button("수집 시작", type="primary", use_container_width=True)

# ============================================================
# 세션 상태 초기화
# ============================================================
if 'collected' not in st.session_state:
    st.session_state.collected = False
    st.session_state.df = None
    st.session_state.errors = []
    st.session_state.csv_data = None
    st.session_state.csv_name = ""
    st.session_state.xlsx_data = None
    st.session_state.xlsx_name = ""
    st.session_state.zip_data = None
    st.session_state.zip_count = 0
    st.session_state.zip_name = ""
    st.session_state.zip_format = ""

# ============================================================
# 메인 실행
# ============================================================
if run_btn and playlist_url:

    st.session_state.collected = False

    # ★ ffmpeg 확인
    has_ffmpeg = check_ffmpeg_available()
    if not has_ffmpeg:
        st.warning("⚠️ ffmpeg가 설치되어 있지 않습니다. "
                    "`--convert-subs`를 비활성화하고 원본 자막 포맷으로 수집합니다.\n\n"
                    "`packages.txt`에 `ffmpeg`를 추가하면 해결됩니다.")

    for d in [SUBTITLE_DIR, CONVERTED_DIR]:
        if os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d, exist_ok=True)

    with st.status("수집 중...", expanded=True) as status:

        # ── 1단계: 영상 ID 수집 ──
        st.write("플레이리스트 분석 중...")
        result = subprocess.run(
            ["yt-dlp", "--flat-playlist", "--dump-json",
             "--no-warnings", "--ignore-errors", playlist_url],
            capture_output=True, text=True, timeout=600,
        )
        flat_entries = []
        for line in result.stdout.strip().split('\n'):
            if line.strip():
                try:
                    flat_entries.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        video_ids = [e.get('id') or e.get('url', '') for e in flat_entries]
        st.write(f"**{len(video_ids)}개** 영상 감지")

        # ── 2단계: 개별 영상 수집 (병렬) ──
        st.write("개별 영상 메타데이터 + 자막 수집 중... (병렬 처리)")
        progress = st.progress(0)
        full_entries = []
        errors = []
        lock = threading.Lock()
        completed_count = 0

        def process_video(idx, vid):
            url = f"https://www.youtube.com/watch?v={vid}"
            entry = None
            error = None

            # --- 메타데이터 ---
            try:
                res_meta = subprocess.run(
                    ["yt-dlp", "--skip-download", "--dump-json",
                     "--no-warnings", "--ignore-errors", url],
                    capture_output=True, text=True, timeout=60,
                )
                if res_meta.stdout.strip():
                    entry = json.loads(res_meta.stdout.strip().split('\n')[0])
                    entry['_playlist_position'] = idx
            except Exception as e:
                error = {'position': idx, 'video_id': vid,
                         'error': f"meta: {str(e)}"}
                return entry, error

            # --- 자막 ---
            # ★ 핵심 수정: -o 템플릿을 자막 전용 옵션으로 변경
            sub_args = [
                "yt-dlp", "--skip-download",
                "--no-warnings", "--ignore-errors",
                "--write-subs",
                # ★ 자막 전용 출력 템플릿 (--output 대신 -o로 지정하되,
                #    자막은 항상 video_id.lang.ext 형태로 저장됨)
                "-o", os.path.join(SUBTITLE_DIR, f"%(id)s.%(ext)s"),
            ]

            # ★ ffmpeg 있을 때만 --convert-subs 사용
            if has_ffmpeg:
                sub_args += ["--convert-subs", INTERNAL_FORMAT]

            if sub_choice == "1":
                sub_args += ["--no-write-auto-subs"]
            elif sub_choice == "2":
                sub_args.remove("--write-subs")
                sub_args += ["--write-auto-subs"]
            elif sub_choice in ("3", "4"):
                sub_args += ["--write-auto-subs"]

            # ★ 핵심 수정: sub-langs에 와일드카드 패턴 추가
            # "ko"만 지정하면 "ko-orig", "ko-auto" 등을 놓침
            if sub_lang.lower() == "all":
                sub_args += ["--sub-langs", "all,-live_chat"]
            else:
                # "ko" → "ko,ko-*,ko.*" 패턴으로 확장
                langs = [l.strip() for l in sub_lang.split(',')]
                expanded = []
                for l in langs:
                    expanded.append(l)
                    expanded.append(f"{l}-*")  # ko-orig, ko-auto 등
                lang_str = ','.join(expanded) + ',-live_chat'
                sub_args += ["--sub-langs", lang_str]

            sub_args.append(url)

            sub_error_msg = ""
            try:
                sub_result = subprocess.run(
                    sub_args, capture_output=True, text=True, timeout=120
                )
                sub_error_msg = sub_result.stderr
            except Exception as e:
                sub_error_msg = str(e)

            # ★ 디버깅: 자막 다운로드 실패 시 에러 정보 수집
            sub_files_for_vid = glob.glob(
                os.path.join(SUBTITLE_DIR, f"{vid}.*")
            )
            if not sub_files_for_vid and sub_error_msg:
                # 치명적이지 않으므로 에러 목록에는 추가하지 않되,
                # entry에 디버깅 정보 첨부
                if entry:
                    entry['_sub_debug'] = sub_error_msg[:500]

            return entry, error

        total = len(video_ids)

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {
                executor.submit(process_video, idx, vid): (idx, vid)
                for idx, vid in enumerate(video_ids, 1)
            }
            for future in as_completed(futures):
                entry, error = future.result()
                with lock:
                    if entry:
                        full_entries.append(entry)
                    if error:
                        errors.append(error)
                    completed_count += 1
                    progress.progress(
                        completed_count / total,
                        text=f"[{completed_count}/{total}] 완료"
                    )

        full_entries.sort(key=lambda x: x.get('_playlist_position', 0))
        progress.progress(1.0, text="수집 완료!")

        # ★ 수정: ffmpeg 없으면 srt 대신 vtt 등 원본 포맷도 검색
        if has_ffmpeg:
            srt_files_found = glob.glob(
                os.path.join(SUBTITLE_DIR, f"*.{INTERNAL_FORMAT}")
            )
        else:
            # ffmpeg 없으면 모든 자막 파일 검색
            all_sub_files = []
            for ext in ["srt", "vtt", "srv1", "srv2", "srv3",
                        "ttml", "ass", "ssa", "json3", "lrc"]:
                all_sub_files.extend(
                    glob.glob(os.path.join(SUBTITLE_DIR, f"*.{ext}"))
                )
            srt_files_found = all_sub_files
            if srt_files_found:
                # 실제로 발견된 확장자 파악
                found_ext = os.path.splitext(srt_files_found[0])[1].lstrip('.')
                INTERNAL_FORMAT_ACTUAL = found_ext
            else:
                INTERNAL_FORMAT_ACTUAL = INTERNAL_FORMAT

        st.write(f"자막 파일 **{len(srt_files_found)}개** 수집됨")

        # ★ 디버깅: 자막이 0개면 디렉토리 내용 표시
        if len(srt_files_found) == 0:
            all_files_in_dir = os.listdir(SUBTITLE_DIR) if os.path.exists(SUBTITLE_DIR) else []
            if all_files_in_dir:
                st.warning(
                    f"자막 디렉토리에 {len(all_files_in_dir)}개 파일이 있으나 "
                    f"예상 확장자와 불일치합니다.\n\n"
                    f"발견된 파일 예시: {all_files_in_dir[:5]}"
                )
            else:
                # 첫 번째 영상의 디버그 정보 표시
                debug_entries = [e for e in full_entries if e.get('_sub_debug')]
                if debug_entries:
                    st.warning(
                        f"자막 다운로드 실패. yt-dlp 메시지 예시:\n\n"
                        f"`{debug_entries[0]['_sub_debug'][:300]}`"
                    )
                else:
                    st.info(
                        "자막 파일이 수집되지 않았습니다. "
                        "해당 영상들에 요청한 언어의 자막이 없을 수 있습니다."
                    )

        # ── 3단계: txt/docx 변환 ──
        final_sub_dir = SUBTITLE_DIR
        # ★ ffmpeg 없는 경우 실제 발견된 포맷 사용
        actual_internal = INTERNAL_FORMAT
        if not has_ffmpeg and srt_files_found:
            actual_internal = os.path.splitext(srt_files_found[0])[1].lstrip('.')
        final_sub_ext = actual_internal
        converted_count = 0

        if output_format in ("txt", "docx") and srt_files_found:
            st.write(f"{output_format.upper()} 변환 중...")
            if output_format == "docx":
                from docx import Document
                from docx.shared import Pt

            for fpath in srt_files_found:
                fname = os.path.basename(fpath)
                # 확장자 제거
                name_base = os.path.splitext(fname)[0]
                # 추가 확장자도 제거 (예: ID.ko → ID)
                vid_from_file = name_base.split('.')[0]

                with open(fpath, 'r', encoding='utf-8', errors='replace') as f:
                    raw_content = f.read()

                plain = srt_to_plain_text(raw_content)
                matched_entry = next(
                    (e for e in full_entries if e.get('id') == vid_from_file),
                    {}
                )
                title = matched_entry.get('title', vid_from_file)
                safe_name = re.sub(r'[^\w가-힣\s]', '', title)[:50].strip()

                if output_format == "txt":
                    out_path = os.path.join(
                        CONVERTED_DIR,
                        f"{vid_from_file}_{safe_name}.txt"
                    )
                    with open(out_path, 'w', encoding='utf-8') as f:
                        f.write(f"제목: {title}\n")
                        f.write(f"영상: https://www.youtube.com/watch?v="
                                f"{vid_from_file}\n")
                        f.write(f"{'=' * 60}\n\n")
                        f.write(plain)
                    converted_count += 1

                elif output_format == "docx":
                    doc = Document()
                    style = doc.styles['Normal']
                    style.font.size = Pt(10)
                    style.paragraph_format.line_spacing = 1.5
                    doc.add_heading(title, level=1)
                    meta_p = doc.add_paragraph()
                    meta_p.add_run("영상: ").bold = True
                    meta_p.add_run(
                        f"https://www.youtube.com/watch?v={vid_from_file}"
                    )
                    doc.add_paragraph('─' * 40)
                    sentences = plain.split('. ')
                    buffer = []
                    for s in sentences:
                        buffer.append(s.strip())
                        if len(buffer) >= 4:
                            doc.add_paragraph('. '.join(buffer) + '.')
                            buffer = []
                    if buffer:
                        doc.add_paragraph('. '.join(buffer))
                    out_path = os.path.join(
                        CONVERTED_DIR,
                        f"{vid_from_file}_{safe_name}.docx"
                    )
                    doc.save(out_path)
                    converted_count += 1

            final_sub_dir = CONVERTED_DIR
            final_sub_ext = output_format
            st.write(f"변환 완료: **{converted_count}개**")

        elif output_format == "vtt" and has_ffmpeg and srt_files_found:
            # srt → vtt 변환
            st.write("VTT 변환 중...")
            for fpath in srt_files_found:
                fname = os.path.basename(fpath)
                out_name = fname.rsplit('.', 1)[0] + '.vtt'
                out_path = os.path.join(CONVERTED_DIR, out_name)
                try:
                    subprocess.run(
                        ["ffmpeg", "-i", fpath, out_path],
                        capture_output=True, timeout=30
                    )
                    converted_count += 1
                except Exception:
                    shutil.copy(fpath, os.path.join(CONVERTED_DIR, fname))
                    converted_count += 1
            final_sub_dir = CONVERTED_DIR
            final_sub_ext = "vtt"

        status.update(
            label=f"수집 완료: {len(full_entries)}개 영상",
            state="complete"
        )

    # ── 4단계: DataFrame ──
    rows = []
    for entry in full_entries:
        vid = entry.get('id', '')

        # ★ 수정: ffmpeg 없을 때는 실제 확장자로 검색
        srt_dict = read_subtitle_files(vid, SUBTITLE_DIR, actual_internal)
        # fallback: 다른 확장자도 시도
        if not srt_dict:
            for try_ext in ["srt", "vtt", "srv1", "ttml", "json3"]:
                srt_dict = read_subtitle_files(vid, SUBTITLE_DIR, try_ext)
                if srt_dict:
                    break

        subtitle_plain = {
            lang: srt_to_plain_text(c) for lang, c in srt_dict.items()
        }
        manual_subs = (
            list(entry.get('subtitles', {}).keys())
            if entry.get('subtitles') else []
        )
        auto_subs = (
            list(entry.get('automatic_captions', {}).keys())
            if entry.get('automatic_captions') else []
        )
        chapters = entry.get('chapters', [])
        chapters_str = ' | '.join(
            [f"{format_duration(ch.get('start_time', 0))} "
             f"{ch.get('title', '')}" for ch in chapters]
        ) if chapters else ''
        thumbnails = entry.get('thumbnails', [])
        best_thumb = thumbnails[-1].get('url', '') if thumbnails else ''

        row = {
            '#': entry.get('_playlist_position', ''),
            'video_url': f"https://www.youtube.com/watch?v={vid}",
            'video_id': vid,
            'title': entry.get('title', ''),
            'description': entry.get('description', ''),
            'channel': entry.get('channel', ''),
            'channel_id': entry.get('channel_id', ''),
            'channel_url': entry.get('channel_url', ''),
            'uploader': entry.get('uploader', ''),
            'channel_follower_count': entry.get('channel_follower_count', ''),
            'upload_date': entry.get('upload_date', ''),
            'view_count': entry.get('view_count', ''),
            'like_count': entry.get('like_count', ''),
            'comment_count': entry.get('comment_count', ''),
            'duration_seconds': entry.get('duration', ''),
            'duration_readable': format_duration(entry.get('duration')),
            'categories': ', '.join(entry.get('categories') or []),
            'tags': ', '.join(entry.get('tags') or []),
            'language': entry.get('language', ''),
            'age_limit': entry.get('age_limit', ''),
            'live_status': entry.get('live_status', ''),
            'availability': entry.get('availability', ''),
            'thumbnail_url': best_thumb,
            'chapters': chapters_str,
            'manual_subtitle_langs':
                ', '.join(manual_subs[:30]) if manual_subs else '',
            'auto_subtitle_langs':
                ', '.join(auto_subs[:15]) if auto_subs else '',
            'subtitle_collected_langs': ', '.join(subtitle_plain.keys()),
        }
        for lang, text in subtitle_plain.items():
            row[f'subtitle_text_{lang}'] = text
        rows.append(row)

    if rows:
        df = pd.DataFrame(rows)
    else:
        df = pd.DataFrame(columns=[
            '#', 'video_url', 'video_id', 'title', 'description', 'channel',
            'channel_id', 'channel_url', 'uploader', 'channel_follower_count',
            'upload_date', 'view_count', 'like_count', 'comment_count',
            'duration_seconds', 'duration_readable', 'categories', 'tags',
            'language', 'age_limit', 'live_status', 'availability',
            'thumbnail_url', 'chapters', 'manual_subtitle_langs',
            'auto_subtitle_langs', 'subtitle_collected_langs',
        ])

    # ── 다운로드 데이터를 session_state에 저장 ──
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    st.session_state.csv_data = df.to_csv(
        index=False, encoding='utf-8-sig'
    ).encode('utf-8-sig')
    st.session_state.csv_name = f"playlist_{timestamp}.csv"

    xlsx_buf = BytesIO()
    with pd.ExcelWriter(xlsx_buf, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Videos')
        sub_cols = [c for c in df.columns if c.startswith('subtitle_text_')]
        if sub_cols:
            df[['#', 'video_id', 'title'] + sub_cols].to_excel(
                writer, index=False, sheet_name='Subtitles')
    st.session_state.xlsx_data = xlsx_buf.getvalue()
    st.session_state.xlsx_name = f"playlist_{timestamp}.xlsx"

    zip_data, zip_count = zip_directory(final_sub_dir, final_sub_ext)
    st.session_state.zip_data = zip_data if zip_count > 0 else None
    st.session_state.zip_count = zip_count
    st.session_state.zip_name = f"subtitles_{output_format}_{timestamp}.zip"
    st.session_state.zip_format = output_format

    st.session_state.df = df
    st.session_state.errors = errors
    st.session_state.collected = True

# ============================================================
# 결과 표시 & 다운로드 (session_state 기반)
# ============================================================
if (st.session_state.collected
        and st.session_state.df is not None
        and not st.session_state.df.empty):
    df = st.session_state.df
    errors = st.session_state.errors

    if 'subtitle_collected_langs' in df.columns:
        sub_count = (
            df['subtitle_collected_langs'].astype(str).str.len() > 0
        ).sum()
    else:
        sub_count = 0

    c1, c2, c3 = st.columns(3)
    c1.metric("총 영상", f"{len(df)}개")
    c2.metric("자막 수집", f"{sub_count}개")
    c3.metric("실패", f"{len(errors)}개")

    display_cols = [
        '#', 'title', 'channel', 'duration_readable',
        'view_count', 'like_count', 'subtitle_collected_langs'
    ]
    display_cols = [c for c in display_cols if c in df.columns]

    st.dataframe(df[display_cols], use_container_width=True, height=400)

    st.subheader("다운로드")
    d1, d2, d3 = st.columns(3)

    with d1:
        st.markdown(
            make_download_link(
                st.session_state.csv_data,
                st.session_state.csv_name,
                "CSV"
            ),
            unsafe_allow_html=True,
        )

    with d2:
        st.markdown(
            make_download_link(
                st.session_state.xlsx_data,
                st.session_state.xlsx_name,
                "XLSX"
            ),
            unsafe_allow_html=True,
        )

    with d3:
        if st.session_state.zip_data:
            st.markdown(
                make_download_link(
                    st.session_state.zip_data,
                    st.session_state.zip_name,
                    f"자막 ZIP ({st.session_state.zip_format}, "
                    f"{st.session_state.zip_count}개)"
                ),
                unsafe_allow_html=True,
            )

    if errors:
        with st.expander(f"실패 로그 ({len(errors)}건)"):
            st.dataframe(pd.DataFrame(errors))

elif run_btn and not playlist_url:
    st.warning("플레이리스트 URL을 입력하세요.")
