import datetime
import json
import os
import re
import time
from dataclasses import dataclass, field
from datetime import timedelta, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

import requests

# 設定參數
GAS_KEY = os.getenv("GAS_KEY")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN_WM")
TG_CHAT_ID = os.getenv("TG_CHAT_ID_WM_TRASH")
HEARTBEAT_URL = os.getenv("HEARTBEAT_URL")

SPREADSHEET_IDS = [
    os.environ.get("SPREADSHEET_ID_MUSE", ""),
    os.environ.get("SPREADSHEET_ID_MEDIALINK", ""),
    os.environ.get("SPREADSHEET_ID_TROPIC", ""),
]
TZ_UTC_PLUS_8 = timezone(timedelta(hours=8))
DEFAULT_FONT_COLOR = "#000000"
ERROR_FONT_COLOR = "#ff0000"
DEFAULT_REGION_BLOCK_SIZE = 6


# ==========================================
# 0. 資料模型
# ==========================================
@dataclass
class OffsetRule:
    match_keywords: List[str] = field(default_factory=list)
    offset_range: Optional[Tuple[float, float]] = None
    include_keywords: List[str] = field(default_factory=list)
    exclude_keywords: List[str] = field(default_factory=list)
    playlist_order: str = "auto"
    invalid_json: bool = False
    raw: str = ""


@dataclass
class RegionDefinition:
    name: str
    start_col: int
    rank_col: Optional[int]
    avg_col: Optional[int]
    total_col: Optional[int]
    first_col: Optional[int]
    link_col: Optional[int]
    offset_col: Optional[int]


@dataclass
class RegionRowData:
    definition: RegionDefinition
    row_num: int
    anime_name: str
    link_urls: List[str]
    offset_raw: Optional[str]

    @property
    def rank_cell(self) -> Optional[str]:
        return make_a1(self.definition.rank_col, self.row_num)

    @property
    def avg_view_cell(self) -> Optional[str]:
        return make_a1(self.definition.avg_col, self.row_num)

    @property
    def total_view_cell(self) -> Optional[str]:
        return make_a1(self.definition.total_col, self.row_num)

    @property
    def first_view_cell(self) -> Optional[str]:
        return make_a1(self.definition.first_col, self.row_num)

    @property
    def offset_cell(self) -> Optional[str]:
        return make_a1(self.definition.offset_col, self.row_num)


@dataclass
class AnimeRow:
    row_num: int
    anime_name: str
    row_signature: str
    ep_count_cell: str
    comp_rank_cell: Optional[str]
    regions: Dict[str, RegionRowData]


@dataclass
class SheetModel:
    sheet_name: str
    regions: List[RegionDefinition]
    rows: List[AnimeRow]


@dataclass
class RegionStats:
    avg: object = "---"
    total: object = "---"
    first: object = "---"
    valid_count: int = 0


# ==========================================
# 1. Google Apps Script API 類別
# ==========================================
class AnimeAPI:
    def __init__(self, script_url, ss_id):
        self.script_url = script_url
        self.ss_id = ss_id

    def _call(self, action, payload=None):
        payload = payload or {}
        payload["action"] = action
        payload["ss_id"] = self.ss_id

        try:
            response = requests.post(
                self.script_url,
                json=payload,
                headers={"Content-Type": "application/json"},
            )
            if response.status_code != 200:
                # 處理 HTTP 錯誤，避免誤把 HTML/轉址頁當成 JSON
                raise Exception(f"HTTP Error: {response.status_code}")

            result = response.json()
            if result.get("status") != "success":
                raise Exception(f"API Error: {result.get('message')}")
            return result.get("data")
        except json.JSONDecodeError:
            raise Exception(f"Invalid JSON response: {response.text[:100]}...")

    def get_schedule(self):
        return self._call("get_schedule")

    def get_sheet_snapshot(self, sheet_name):
        return self._call("get_sheet_snapshot", {"sheet_name": sheet_name})

    def batch_update(self, sheet_name, update_list):
        return self._call("update_data", {"sheet_name": sheet_name, "data": update_list})

    def sort_data(self, sheet_name):
        return self._call("sort_data", {"sheet_name": sheet_name})

    def update_system_time(self, time_str):
        return self._call("update_timestamp", {"time_str": time_str})


class YouTubeDataProcessor:
    def __init__(self, api_key):
        self.api_key = api_key
        self.quota_exceeded = False
        # 忽略關鍵字清單
        self.ignore_keywords = [
            "預告",
            "PV",
            "NCED",
            "NCOP",
            "OP",
            "ED",
            "Creditless",
            "Trailer",
            "Teaser",
            "Recap",
            "Special",
            "特別篇",
            "總集篇",
            "全集馬拉松",
            "Semua Episode",
            "Chia sẻ của DV lồng tiếng",
            "CM",
            "第二季製作決定",
            "精華重溫",
            "Preview",
        ]
        # 有例外動畫名稱的關鍵字
        self.ignore_keywords_exceptions = {"CM": ["testCM動畫名稱"]}
        self.youtube_base_url = "https://www.googleapis.com/youtube/v3"
        # 同一次執行中，播放清單與影片統計都盡量重用快取，節省 API 配額
        self.playlist_cache: Dict[str, object] = {}
        self.video_stats_cache: Dict[str, Optional[int]] = {}

    def _check_quota(self, response):
        if response.status_code == 403:
            try:
                error_data = response.json()
                reasons = [e.get("reason") for e in error_data.get("error", {}).get("errors", [])]
                if "quotaExceeded" in reasons or "dailyLimitExceeded" in reasons:
                    print("!!! YouTube API Quota Exceeded. Stopping all requests. !!!")
                    self.quota_exceeded = True
                    return True
            except Exception:
                pass
        return False

    def get_playlist_id(self, url):
        """從連結中提取 Playlist ID"""
        if not url:
            return None
        try:
            query = parse_qs(urlparse(url).query)
            if "list" in query:
                return query["list"][0]
        except Exception:
            pass
        return None

    def get_playlist_items(self, playlist_id, playlist_sequence=0):
        """取得播放清單中的所有影片 ID、標題與在清單中的位置"""
        if self.quota_exceeded:
            return None

        cached = self.playlist_cache.get(playlist_id)
        if cached == "REMOVED":
            return "REMOVED"
        if isinstance(cached, list):
            # playlist_sequence 代表同一格有多個播放清單時的原始順序
            return [{**item, "playlist_sequence": playlist_sequence} for item in cached]

        items = []
        next_page_token = ""

        while True:
            url = (
                f"{self.youtube_base_url}/playlistItems?part=snippet&maxResults=50"
                f"&playlistId={playlist_id}&key={self.api_key}&hl=zh-Hant"
            )
            if next_page_token:
                url += f"&pageToken={next_page_token}"

            res = requests.get(url)
            if self._check_quota(res):
                return None
            if res.status_code == 404:
                # 404 視為播放清單已移除，快取結果避免重複打 API
                self.playlist_cache[playlist_id] = "REMOVED"
                return "REMOVED"
            if res.status_code != 200:
                print(f"Error fetching playlist {playlist_id}: {res.status_code}")
                self.playlist_cache[playlist_id] = []
                return []

            data = res.json()
            for item in data.get("items", []):
                snippet = item.get("snippet", {})
                vid = snippet.get("resourceId", {}).get("videoId")
                if vid:
                    items.append(
                        {
                            "id": vid,
                            "title": snippet.get("title", ""),
                            "playlist_id": playlist_id,
                            "position": int(snippet.get("position", 0)),
                        }
                    )

            next_page_token = data.get("nextPageToken")
            if not next_page_token:
                break

        self.playlist_cache[playlist_id] = items
        return [{**item, "playlist_sequence": playlist_sequence} for item in items]

    def get_video_stats(self, video_ids):
        """批量取得影片統計資料 (ViewCount)，並過濾尚未首播的影片"""
        if self.quota_exceeded or not video_ids:
            return {}

        stats_map = {}
        pending_ids = []

        for vid in video_ids:
            if vid in self.video_stats_cache:
                stats_map[vid] = self.video_stats_cache[vid]
            else:
                pending_ids.append(vid)

        now_utc = datetime.datetime.now(datetime.timezone.utc)
        # 每次最多 50 筆
        for i in range(0, len(pending_ids), 50):
            batch = pending_ids[i : i + 50]
            ids_str = ",".join(batch)
            url = (
                f"{self.youtube_base_url}/videos?part=statistics,liveStreamingDetails"
                f"&id={ids_str}&key={self.api_key}&hl=zh-Hant"
            )
            res = requests.get(url)
            if self._check_quota(res):
                return stats_map
            if res.status_code != 200:
                print(f"Error fetching video stats: {res.status_code}")
                continue

            seen_ids = set()
            for item in res.json().get("items", []):
                vid = item.get("id")
                seen_ids.add(vid)

                live_details = item.get("liveStreamingDetails")
                if live_details and live_details.get("scheduledStartTime"):
                    try:
                        # 如果是未來的首播，先標記成 None，避免提前算進觀看數
                        dt_start = datetime.datetime.fromisoformat(
                            live_details["scheduledStartTime"].replace("Z", "+00:00")
                        )
                        if dt_start > now_utc:
                            self.video_stats_cache[vid] = None
                            stats_map[vid] = None
                            continue
                    except ValueError:
                        pass

                view_count = item.get("statistics", {}).get("viewCount")
                normalized = int(view_count) if view_count else None
                self.video_stats_cache[vid] = normalized
                stats_map[vid] = normalized

            for missing_vid in batch:
                if missing_vid not in seen_ids:
                    self.video_stats_cache[missing_vid] = None

        for vid in video_ids:
            if vid not in stats_map and vid in self.video_stats_cache:
                stats_map[vid] = self.video_stats_cache[vid]
        return stats_map

    def parse_episode_number(self, title):
        """
        從標題解析集數
        回傳: int 或 float (不減 1，直接回傳原始數值)
        """

        patterns = [
            # 1. 中文規則 (例如: 第1集, 第 12 話, 第1.5話)
            r"第\s*(\d+(?:\.\d+)?)\s*[集話]",
            # 2. 英文規則 (例如: Episode 5, Episode.05)
            r"Episode\s*[\.]?\s*(\d+(?:\.\d+)?)",
            # 3. 泰文規則 (例如: ตอนที่ 5)
            r"ตอนที่\s*(\d+(?:\.\d+)?)",
            # 4. 越文規則 (例如: Tập 5)
            r"Tập\s*(\d+(?:\.\d+)?)",
            # 5. 印尼文規則 (例如: Misi 5)
            r"Misi\s*(\d+(?:\.\d+)?)",
            # 6. 馬來文規則 (例如: Episod 5)
            r"Episod\s*[\.]?\s*(\d+(?:\.\d+)?)",
            # 0. 簡寫規則 (例如: EP.5, EP 05)
            r"EP\s*[\.]?\s*(\d+(?:\.\d+)?)",
            # 0. 方括號規則 (例如: [12], [05]) - 視情況開啟，可能會誤判年份
            # r'\[(\d+(?:\.\d+)?)\]',
            # 0. 純井號規則 (例如: #12)
            # r'#\s*(\d+(?:\.\d+)?)',
        ]

        for pattern in patterns:
            # re.IGNORECASE 讓 EP/ep/Episode/episode 都能抓到
            match = re.search(pattern, title, re.IGNORECASE)
            if not match:
                continue
            try:
                num = float(match.group(1))
                return int(num) if num.is_integer() else num
            except Exception:
                continue
        return None

    def is_ignored_keyword(self, title):
        """檢查是否包含忽略關鍵字"""
        for kw in self.ignore_keywords:
            if kw in self.ignore_keywords_exceptions:
                if any(exc in title for exc in self.ignore_keywords_exceptions[kw]):
                    continue
            if kw in title:
                return True
        return False

    def parse_offset_rule(self, raw_value):
        """
        解析每個國家區塊自己的 OFFSET 規則。
        支援：
        1. 空字串
        2. 純字串 -> match
        3. JSON 陣列 -> offset range
        4. JSON 物件 -> 複合規則
        """
        text = "" if raw_value is None else str(raw_value).strip()
        rule = OffsetRule(raw=text)
        if not text:
            return rule

        if text.startswith("{") and text.endswith("}"):
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                # 長得像 JSON 物件但解析失敗，交由後續回寫標紅
                rule.invalid_json = True
                return rule

            if not isinstance(payload, dict):
                return rule

            # match 可為單字串，也可為 ["關鍵詞1", "關鍵詞2"] 多關鍵詞
            rule.match_keywords = self._normalize_keyword_list(payload.get("match"))
            rule.offset_range = self._normalize_offset_range(payload.get("offset"))
            rule.include_keywords = self._normalize_keyword_list(payload.get("include"))
            rule.exclude_keywords = self._normalize_keyword_list(payload.get("exclude"))
            playlist_order = str(payload.get("playlist_order", "auto")).lower()
            rule.playlist_order = playlist_order if playlist_order in {"auto", "asc", "desc"} else "auto"
            return rule

        if text.startswith("[") and text.endswith("]"):
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                # 舊資料若方括號格式壞掉，退回純字串 match，避免整列直接失效
                return OffsetRule(match_keywords=[text], raw=text)

            normalized = self._normalize_offset_range(payload)
            if normalized:
                rule.offset_range = normalized
                return rule

        rule.match_keywords = [text]
        return rule

    def should_include_video(self, title, rule):
        """
        規則套用順序固定為：
        1. 結構性忽略詞
        2. exclude
        3. include
        4. match
        5. offset
        """
        if self.is_ignored_keyword(title):
            return False

        if rule.exclude_keywords and any(keyword in title for keyword in rule.exclude_keywords):
            return False

        if rule.include_keywords and not any(keyword in title for keyword in rule.include_keywords):
            return False

        # match 陣列採 AND：所有關鍵詞都必須命中
        if rule.match_keywords and not all(keyword in title for keyword in rule.match_keywords):
            return False

        if rule.offset_range is not None:
            ep_idx = self.parse_episode_number(title)
            if ep_idx is None:
                return False
            start, end = rule.offset_range
            if not (start <= ep_idx <= end):
                return False

        return True

    def process_region(self, region_data, rule):
        """處理單一作品在單一國家區塊內的觀看量計算"""
        if self.quota_exceeded:
            return None

        result = RegionStats()
        if not region_data.link_urls:
            return None

        all_items = []
        has_valid_playlist = False

        for playlist_sequence, url in enumerate(region_data.link_urls):
            playlist_id = self.get_playlist_id(url)
            if not playlist_id:
                continue

            has_valid_playlist = True
            # 取得播放清單，並保留同一儲存格內多個清單的先後順序
            items = self.get_playlist_items(playlist_id, playlist_sequence)
            if items == "REMOVED":
                continue
            if items:
                all_items.extend(items)

        if not has_valid_playlist:
            return None
        if not all_items:
            return result

        valid_videos = []
        for item in all_items:
            if self.should_include_video(item["title"], rule):
                valid_videos.append(item)

        if not valid_videos:
            return result

        # 去重複影片（依 video id）
        deduped_videos = []
        seen_ids = set()
        for video in valid_videos:
            if video["id"] in seen_ids:
                continue
            seen_ids.add(video["id"])
            deduped_videos.append(video)
        valid_videos = deduped_videos

        stats = self.get_video_stats([video["id"] for video in valid_videos])
        # 過濾不存在 / private / deleted 影片
        valid_videos = [video for video in valid_videos if video["id"] in stats]
        if not valid_videos:
            return result
        if self.quota_exceeded:
            return None

        playlist_orders = self._detect_playlist_orders(valid_videos, rule.playlist_order)
        playlist_max_positions = {}
        for video in valid_videos:
            playlist_id = video["playlist_id"]
            playlist_max_positions[playlist_id] = max(
                playlist_max_positions.get(playlist_id, 0),
                int(video.get("position", 0)),
            )

        # 首集判定優先看最小集數，完全無法解析時才回退到播放清單順序
        first_video = self._select_first_video(valid_videos, playlist_orders, playlist_max_positions)

        # 計算總量與平均
        total_views = 0
        valid_view_count = 0
        has_any_view_data = False
        for video in valid_videos:
            views = stats.get(video["id"])
            if views is None:
                continue
            has_any_view_data = True
            total_views += views
            valid_view_count += 1

        if not has_any_view_data:
            result.valid_count = len(valid_videos)
            if first_video is not None:
                # 全部影片都沒可用觀看數時，首集仍盡量反映該影片是否為會員限定
                first_views = stats.get(first_video["id"])
                result.first = first_views if first_views is not None else "---"
            return result

        result.total = total_views
        result.avg = int(total_views / valid_view_count) if valid_view_count > 0 else 0
        result.valid_count = len(valid_videos)
        if first_video is not None:
            first_views = stats.get(first_video["id"])
            result.first = first_views if first_views is not None else "---"
        return result

    def _normalize_offset_range(self, value):
        """將 offset 正規化為 [start, end]"""
        if not isinstance(value, list) or len(value) != 2:
            return None
        try:
            start = float(value[0])
            end = float(value[1])
            return (start, end)
        except (TypeError, ValueError):
            return None

    def _normalize_keyword_list(self, value):
        """將 include / exclude 正規化成字串陣列"""
        if isinstance(value, str):
            return [value] if value else []
        if isinstance(value, list):
            return [str(item) for item in value if str(item)]
        return []

    def _detect_playlist_orders(self, videos, preferred_order):
        """
        自動判斷播放清單是正序還是倒序。
        如果已有明確指定 asc / desc，就直接沿用。
        """
        grouped = {}
        for video in videos:
            grouped.setdefault(video["playlist_id"], []).append(video)

        if preferred_order in {"asc", "desc"}:
            return {playlist_id: preferred_order for playlist_id in grouped}

        resolved = {}
        for playlist_id, playlist_videos in grouped.items():
            ordered = sorted(playlist_videos, key=lambda item: item.get("position", 0))
            parsed_eps = [self.parse_episode_number(item["title"]) for item in ordered]
            parsed_eps = [ep for ep in parsed_eps if ep is not None]
            # 若第一個可解析集數大於最後一個，可視為倒序清單 (例如 5 4 3 2 1)
            if len(parsed_eps) >= 2 and parsed_eps[0] > parsed_eps[-1]:
                resolved[playlist_id] = "desc"
            else:
                resolved[playlist_id] = "asc"
        return resolved

    def _select_first_video(self, videos, playlist_orders, playlist_max_positions):
        """挑出首集對應的影片，用於填寫首集觀看"""
        if not videos:
            return None

        def normalized_position(video):
            playlist_id = video["playlist_id"]
            playlist_order = playlist_orders.get(playlist_id, "asc")
            position = int(video.get("position", 0))
            if playlist_order == "desc":
                # 倒序清單時，把 position 反轉後再比較
                return playlist_max_positions.get(playlist_id, position) - position
            return position

        annotated = []
        has_episode_number = False
        for video in videos:
            ep_idx = self.parse_episode_number(video["title"])
            if ep_idx is not None:
                has_episode_number = True
            annotated.append(
                {
                    **video,
                    "episode_number": ep_idx,
                    "normalized_position": normalized_position(video),
                }
            )

        if has_episode_number:
            # 優先取最小集數；同集數時，再用正規化後的清單順序打破平手
            annotated.sort(
                key=lambda item: (
                    item["episode_number"] if item["episode_number"] is not None else float("inf"),
                    item["normalized_position"],
                    item.get("playlist_sequence", 0),
                    item.get("position", 0),
                )
            )
        else:
            # 若完全抓不到集數，只能退回到播放清單順序
            annotated.sort(
                key=lambda item: (
                    item["normalized_position"],
                    item.get("playlist_sequence", 0),
                    item.get("position", 0),
                )
            )

        return annotated[0] if annotated else None


# ==========================================
# 2. 試算表快照解析與回寫 payload 組裝
# ==========================================
def make_a1(col_num, row_num):
    if not col_num or row_num <= 0:
        return None
    letters = ""
    while col_num > 0:
        col_num, remainder = divmod(col_num - 1, 26)
        letters = chr(65 + remainder) + letters
    return f"{letters}{row_num}"


def detect_regions(row1, row2):
    """依真實表格格式解析所有國家/區域區塊"""
    regions = []
    last_col = min(len(row1), len(row2))
    col = 5

    while col <= last_col:
        header = str(row1[col - 1]).strip()
        if not header:
            col += 1
            continue
        if "備註" in header:
            break
        if str(row2[col - 1]).strip() != "排名":
            # 避免遇到意外欄位時整體偏移，逐欄往後找下一個合法區塊
            col += 1
            continue

        regions.append(
            RegionDefinition(
                name=header,
                start_col=col,
                rank_col=col,
                avg_col=col + 1,
                total_col=col + 2,
                first_col=col + 3,
                link_col=col + 4,
                offset_col=col + 5,
            )
        )
        col += DEFAULT_REGION_BLOCK_SIZE

    return regions


def detect_comp_rank_col(row1):
    """從第一列找出綜合排名欄"""
    for col_idx, value in enumerate(row1, start=1):
        text = str(value)
        if "綜合排名" in text or "平均流量最多" in text:
            return col_idx
    return None


def parse_sheet_snapshot(sheet_name, snapshot):
    """把 GAS 回傳的原始快照轉成 Python 端的資料模型"""
    row1 = snapshot.get("row1", [])
    row2 = snapshot.get("row2", [])
    rows_payload = snapshot.get("rows", [])
    regions = detect_regions(row1, row2)
    comp_rank_col = detect_comp_rank_col(row1)

    rows = []
    for row_payload in rows_payload:
        values = row_payload.get("values", [])
        anime_name = str(values[0]).strip() if values and values[0] is not None else ""
        if not anime_name:
            continue

        row_num = int(row_payload["row_num"])
        link_urls_by_col = row_payload.get("link_urls_by_col", {})
        region_map = {}
        for region in regions:
            # OFFSET 的作用域是「單一區塊單一儲存格」
            offset_raw = values[region.offset_col - 1] if region.offset_col and len(values) >= region.offset_col else None
            region_map[region.name] = RegionRowData(
                definition=region,
                row_num=row_num,
                anime_name=anime_name,
                link_urls=link_urls_by_col.get(str(region.link_col), []),
                offset_raw=offset_raw,
            )

        rows.append(
            AnimeRow(
                row_num=row_num,
                anime_name=anime_name,
                row_signature=row_payload.get("row_signature", ""),
                ep_count_cell=make_a1(4, row_num),
                comp_rank_cell=make_a1(comp_rank_col, row_num),
                regions=region_map,
            )
        )

    return SheetModel(sheet_name=sheet_name, regions=regions, rows=rows)


def init_row_updates(rows):
    """先為每一列建立待回寫容器，最後再彙整成 GAS payload"""
    payloads = {}
    for row in rows:
        payloads[row.row_num] = {
            "row_check_name": row.anime_name,
            "target_row": row.row_num,
            "expected_row_signature": row.row_signature,
            "_value_map": {},
            "_format_map": {},
            "_has_links": False,
        }
    return payloads


def queue_value_update(row_payload, cell, value):
    """同一格若被重複設定，後者覆蓋前者"""
    if cell:
        row_payload["_value_map"][cell] = value


def queue_format_update(row_payload, cell, font_color):
    """目前只處理字色，但保留 format map 結構方便之後擴充"""
    if cell:
        row_payload["_format_map"][cell] = {"font_color": font_color}


def finalize_row_updates(row_payloads):
    """將內部 map 正規化成 GAS 可直接吃的陣列 payload"""
    finalized = []
    for row_num in sorted(row_payloads):
        payload = row_payloads[row_num]
        value_updates = [{"cell": cell, "value": value} for cell, value in sorted(payload["_value_map"].items())]
        format_updates = []
        for cell, format_data in sorted(payload["_format_map"].items()):
            format_updates.append({"cell": cell, **format_data})

        if not value_updates and not format_updates:
            continue

        finalized.append(
            {
                "row_check_name": payload["row_check_name"],
                "target_row": payload["target_row"],
                "expected_row_signature": payload["expected_row_signature"],
                "value_updates": value_updates,
                "format_updates": format_updates,
            }
        )
    return finalized


def calculate_sheet_updates(sheet_model, processor):
    """計算單張工作表所有區塊的更新結果"""
    row_payloads = init_row_updates(sheet_model.rows)
    row_max_ep_map = {}
    global_avg_sum_map = {}  # 用於計算綜合排名的累加器 { row_idx: total_sum_avg }
    row_has_any_links = {}

    # 遍歷區域
    for region in sheet_model.regions:
        print(f"\n[{sheet_model.sheet_name}] 正在處理區域: {region.name}")
        region_results = []

        # --- 第一階段：讀取該區域所有影片數據 ---
        for row in sheet_model.rows:
            if processor.quota_exceeded:
                return []

            region_data = row.regions[region.name]
            row_payload = row_payloads[row.row_num]
            rule = processor.parse_offset_rule(region_data.offset_raw)

            if region_data.offset_cell:
                # OFFSET JSON 解析失敗就標紅，成功則恢復正常字色
                queue_format_update(
                    row_payload,
                    region_data.offset_cell,
                    ERROR_FONT_COLOR if rule.invalid_json else DEFAULT_FONT_COLOR,
                )

            stats = None
            if region_data.link_urls:
                row_has_any_links[row.row_num] = True
                row_payload["_has_links"] = True
                display_name = row.anime_name.split("\n")[0]
                print(f"  > 處理: {display_name} ...", end=" ", flush=True)
                if rule.invalid_json:
                    print("OFFSET JSON 格式錯誤，改以預設過濾規則繼續。", end=" ", flush=True)

                stats = processor.process_region(region_data, rule)
                if stats is None:
                    print("跳過 (無有效連結/配額滿/API錯誤)")
                else:
                    print(f"總={stats.total}, 均={stats.avg}, 首={stats.first}, 集={stats.valid_count}")
                    # 累加平均流量至全域 Map，之後拿來算綜合排名
                    if isinstance(stats.avg, int):
                        global_avg_sum_map[row.row_num] = global_avg_sum_map.get(row.row_num, 0) + stats.avg
                    # 總集數取同一列所有區塊裡最大的 valid_count
                    if stats.valid_count > row_max_ep_map.get(row.row_num, 0):
                        row_max_ep_map[row.row_num] = stats.valid_count
            elif rule.invalid_json:
                print(f"  > {row.anime_name.split('\n')[0]} 的 {region.name} OFFSET JSON 格式錯誤，已標紅。")

            region_results.append({"row": row, "region_data": region_data, "stats": stats})

        # --- 第二階段：計算該區域排名 ---
        valid_rank_items = [item for item in region_results if item["stats"] and isinstance(item["stats"].avg, int)]
        valid_rank_items.sort(key=lambda item: item["stats"].avg, reverse=True)
        rank_map = {item["row"].row_num: idx + 1 for idx, item in enumerate(valid_rank_items)}

        # --- 第三階段：準備寫入資料 ---
        for item in region_results:
            row = item["row"]
            region_data = item["region_data"]
            stats = item["stats"]
            row_payload = row_payloads[row.row_num]

            if region_data.link_urls and region_data.rank_cell:
                queue_value_update(row_payload, region_data.rank_cell, rank_map.get(row.row_num, ""))

            if not region_data.link_urls or stats is None:
                continue

            # 寫入觀看數據
            queue_value_update(row_payload, region_data.avg_view_cell, stats.avg)
            queue_value_update(row_payload, region_data.total_view_cell, stats.total)
            queue_value_update(row_payload, region_data.first_view_cell, stats.first)

    if not processor.quota_exceeded:
        # --- 第四階段：計算並寫入「綜合排名」 ---
        print(f"\n[{sheet_model.sheet_name}] 正在計算綜合排名...")

        valid_global_list = [(row_num, value) for row_num, value in global_avg_sum_map.items() if value > 0]
        valid_global_list.sort(key=lambda item: item[1], reverse=True)
        global_rank_map = {row_num: idx + 1 for idx, (row_num, _) in enumerate(valid_global_list)}

        for row in sheet_model.rows:
            row_payload = row_payloads[row.row_num]
            # 先更新總集數
            if row.row_num in row_max_ep_map and row_max_ep_map[row.row_num] > 0:
                queue_value_update(row_payload, row.ep_count_cell, row_max_ep_map[row.row_num])

            # 只有該列至少有一個有效區塊連結時，才寫綜合排名
            if row.comp_rank_cell and row_has_any_links.get(row.row_num):
                queue_value_update(row_payload, row.comp_rank_cell, global_rank_map.get(row.row_num, ""))

    return finalize_row_updates(row_payloads)


# ==========================================
# 3. 主程式執行區
# ==========================================
# 初始化
GAS_URL = f"https://script.google.com/macros/s/{GAS_KEY}/exec"
yt_processor = YouTubeDataProcessor(YOUTUBE_API_KEY)


def send_telegram_error(error_msg):
    """發送錯誤訊息到 Telegram"""
    try:
        url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TG_CHAT_ID,
            "text": f"[日本動畫 Youtube亞洲新番觀看量]\n發生錯誤:\n\n{error_msg}",
        }
        requests.post(url, json=payload)
        print("已發送錯誤通知至 Telegram")
    except Exception as e:
        print(f"發送 Telegram 失敗: {e}")


def process_single_sheet(sheet_name, gas_api=None, processor=None):
    """處理單一工作表的完整流程 (讀取 -> 解析 -> 計算 -> 寫入)"""
    gas_api = gas_api or current_gas_api
    processor = processor or yt_processor

    print(f"\n====== 準備處理工作表: {sheet_name} ======")

    # 讀取工作表快照
    try:
        snapshot = gas_api.get_sheet_snapshot(sheet_name)
    except Exception as e:
        print(f"讀取工作表 {sheet_name} 失敗: {e}")
        return

    sheet_model = parse_sheet_snapshot(sheet_name, snapshot)
    if not sheet_model.rows:
        print(f"工作表 {sheet_name} 無資料或格式錯誤，跳過。")
        return

    # 由 Python 端完成所有區塊解析與觀看量計算
    updates_batch = calculate_sheet_updates(sheet_model, processor)

    if processor.quota_exceeded:
        print("\n[暫停] 因為配額已滿，不執行寫入")
        return

    if not updates_batch:
        print(f"\n[資訊] 工作表 {sheet_name} 沒有需要更新的資料")
        return

    # 批量寫入該工作表，GAS 端會再用 row_signature 做一次並發安全檢查
    print(f"\n[寫入] 準備寫入 {len(updates_batch)} 筆資料至 {sheet_name}...")
    try:
        resp = gas_api.batch_update(sheet_name, updates_batch)
        print(
            f"[寫入] 完成! 更新數: {resp.get('updated')}, "
            f"衝突: {len(resp.get('conflicts', []))}, 錯誤: {len(resp.get('errors', []))}"
        )
        if resp.get("conflicts"):
            print("[寫入] 衝突列如下:")
            for conflict in resp["conflicts"][:10]:
                print(f"  - {conflict}")
        if resp.get("errors"):
            print("[寫入] 錯誤如下:")
            for error in resp["errors"][:10]:
                print(f"  - {error}")

        if resp.get("updated", 0) > 0:
            # 只有真的有成功寫入時才排序
            print(f"[排序] 正在對 {sheet_name} 進行綜合排名排序...")
            sort_resp = gas_api.sort_data(sheet_name)
            print(f"[排序] {sort_resp.get('message')}")
    except Exception as e:
        print(f"[寫入/排序] 失敗: {e}")


def main():
    print("=== 開始執行自動化更新任務 ===")
    # 用於捕捉是否有任何錯誤發生
    global_error_occurred = False

    try:
        # 計算今天是星期幾
        now_plus_8 = datetime.datetime.now(TZ_UTC_PLUS_8)
        today_weekday = now_plus_8.weekday() + 1
        today_key = str(today_weekday)
        print(f"今天是星期 {today_weekday} (GAS Key: {today_key})")

        # 遍歷所有試算表 ID
        for idx, ss_id in enumerate(SPREADSHEET_IDS):
            if not ss_id:
                continue

            print(f"\n>>>>>>>> 正在處理第 {idx + 1} 個試算表 (ID: {ss_id}) <<<<<<<<")

            gas_api = AnimeAPI(GAS_URL, ss_id)
            global current_gas_api
            current_gas_api = gas_api

            # 1. 取得排程表
            print("正在讀取更新排程...")
            try:
                schedule = gas_api.get_schedule()
            except Exception as e:
                print(f"讀取排程失敗: {e}")
                raise

            # 2. 決定要更新哪些工作表
            sheets_to_process = set()
            if "0" in schedule:
                sheets_to_process.update(schedule["0"])
            if today_key in schedule:
                sheets_to_process.update(schedule[today_key])

            target_sheets = sorted([str(sheet) for sheet in sheets_to_process])
            if not target_sheets:
                print("此試算表今天沒有安排需要更新的工作表。")
                continue

            print(f"今日待更新列表: {target_sheets}")

            # 3. 逐一處理每個工作表
            for sheet_name in target_sheets:
                if yt_processor.quota_exceeded:
                    print("因配額已滿，停止後續工作表的處理。")
                    raise Exception("YouTube API 配額已滿")

                process_single_sheet(sheet_name)

                if sheet_name != target_sheets[-1]:
                    time.sleep(2)

    except Exception as e:
        # 捕捉所有執行途中的報錯，傳到 TG
        global_error_occurred = True

        import sys
        import traceback

        error_class = e.__class__.__name__
        detail = e.args[0] if e.args else str(e)
        # 取得詳細錯誤訊息
        _, _, tb = sys.exc_info()
        last_call_stack = traceback.extract_tb(tb)[-1]
        file_name = last_call_stack[0]
        line_num = last_call_stack[1]
        func_name = last_call_stack[2]
        err_msg = f'File "{file_name}", line {line_num}, in {func_name}: [{error_class}] {detail}'
        print(f"\n❌ 發生嚴重錯誤: {err_msg}")
        send_telegram_error(err_msg)

    if not global_error_occurred:
        print("\n✅ 所有任務執行完成，且無錯誤。")
        try:
            # 更新所有表格的系統時間
            now_str = datetime.datetime.now(TZ_UTC_PLUS_8).strftime("%Y年%m月%d日 %H:%M")
            print(f"正在更新系統時間標記: {now_str}")

            for ss_id in SPREADSHEET_IDS:
                if not ss_id:
                    continue
                api = AnimeAPI(GAS_URL, ss_id)
                res = api.update_system_time(now_str)
                print(f"  - 試算表 {ss_id[-4:]}... : {res.get('message')}")
        except Exception as e:
            print(f"更新系統時間時發生錯誤: {e}")
            send_telegram_error(f"更新系統時間失敗: {e}")
            return

        if HEARTBEAT_URL:
            try:
                # 發送 Heartbeat
                print("正在發送 Heartbeat...")
                requests.get(HEARTBEAT_URL)
                print("Heartbeat 發送成功")
            except Exception as e:
                print(f"Heartbeat 發送失敗: {e}")


if __name__ == "__main__":
    main()
