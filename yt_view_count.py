import requests
import json
import re
import os
import time
import datetime
from datetime import timezone, timedelta
from urllib.parse import parse_qs, urlparse

# 設定參數
GAS_KEY = os.getenv("GAS_KEY")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN_WM")
TG_CHAT_ID = os.getenv("TG_CHAT_ID_WM_TRASH")
HEARTBEAT_URL = os.getenv("HEARTBEAT_URL")

SPREADSHEET_IDS = [
    os.environ.get("SPREADSHEET_ID_MUSE", ""),
    os.environ.get("SPREADSHEET_ID_MEDIALINK", ""),
    os.environ.get("SPREADSHEET_ID_TROPIC", "")
]
TZ_UTC_PLUS_8 = timezone(timedelta(hours=8))

# ==========================================
# 1. Google Apps Script API 類別
# ==========================================
class AnimeAPI:
    def __init__(self, script_url, ss_id):
        self.script_url = script_url
        self.ss_id = ss_id

    def _call(self, action, payload=None):
        if payload is None:
            payload = {}
        payload['action'] = action
        payload['ss_id'] = self.ss_id

        try:
            response = requests.post(
                self.script_url, 
                json=payload,
                headers={'Content-Type': 'application/json'}
            )

            if response.status_code != 200:
                # 處理 302 轉址後的 HTML 錯誤或其他 HTTP 錯誤
                raise Exception(f"HTTP Error: {response.status_code}")
                
            result = response.json()

            if result.get('status') != 'success':
                raise Exception(f"API Error: {result.get('message')}")
                
            return result.get('data')

        except json.JSONDecodeError:
            raise Exception(f"Invalid JSON response: {response.text[:100]}...")

    def get_schedule(self):
        return self._call("get_schedule")

    def get_sheet_data(self, sheet_name):
        return self._call("get_sheet_data", {"sheet_name": sheet_name})

    def batch_update(self, sheet_name, update_list):
        return self._call("update_data", {
            "sheet_name": sheet_name,
            "data": update_list
        })

    def sort_data(self, sheet_name):
        return self._call("sort_data", {
            "sheet_name": sheet_name
        })

    def update_system_time(self, time_str):
        return self._call("update_timestamp", {
            "time_str": time_str
        })

# ==========================================
# 2. YouTube 資料處理類別
# ==========================================
class YouTubeDataProcessor:
    def __init__(self, api_key):
        self.api_key = api_key
        self.quota_exceeded = False
        # 忽略關鍵字清單
        self.ignore_keywords = ["預告", "PV", "NCED", "NCOP", "OP", "ED", "Creditless", "Trailer", "Teaser", "Recap", "Special", "特別篇", "總集篇", "全集馬拉松",
                                "Semua Episode", "Chia sẻ của DV lồng tiếng", "CM", "第二季製作決定", "精華重溫", "Preview"]
        # 有例外動畫名稱的關鍵字
        self.ignore_keywords_exceptions = {
            "CM": ["testCM動畫名稱"]
        }
        self.youtube_base_url = "https://www.googleapis.com/youtube/v3"

    def _check_quota(self, response):
        if response.status_code == 403:
            try:
                error_data = response.json()
                reasons = [e.get('reason') for e in error_data.get('error', {}).get('errors', [])]
                if 'quotaExceeded' in reasons or 'dailyLimitExceeded' in reasons:
                    print("!!! YouTube API Quota Exceeded. Stopping all requests. !!!")
                    self.quota_exceeded = True
                    return True
            except:
                pass
        return False

    def get_playlist_id(self, url):
        """從連結中提取 Playlist ID"""
        if not url: return None
        try:
            query = parse_qs(urlparse(url).query)
            if 'list' in query:
                return query['list'][0]
        except:
            pass
        return None

    def get_playlist_items(self, playlist_id):
        """取得播放清單中的所有影片 ID 與標題"""
        if self.quota_exceeded: return None

        items = []
        next_page_token = ""

        while True:
            url = f"{self.youtube_base_url}/playlistItems?part=snippet&maxResults=50&playlistId={playlist_id}&key={self.api_key}&hl=zh-Hant"
            if next_page_token:
                url += f"&pageToken={next_page_token}"

            res = requests.get(url)

            # 檢查配額
            if self._check_quota(res): return None

            # 檢查是否下架 (404)
            if res.status_code == 404:
                return "REMOVED"

            if res.status_code != 200:
                print(f"Error fetching playlist {playlist_id}: {res.status_code}")
                return [] # 視為空或錯誤

            data = res.json()

            for item in data.get('items', []):
                snippet = item.get('snippet', {})
                title = snippet.get('title', '')
                vid = snippet.get('resourceId', {}).get('videoId')
                if vid:
                    items.append({"id": vid, "title": title})

            next_page_token = data.get('nextPageToken')
            if not next_page_token:
                break

        return items

    def get_video_stats(self, video_ids):
        """批量取得影片統計資料 (ViewCount)，並過濾尚未首播的影片"""
        if self.quota_exceeded or not video_ids: return {}

        stats_map = {}
        now_utc = datetime.datetime.now(datetime.timezone.utc)

        # 每次最多 50 筆
        for i in range(0, len(video_ids), 50):
            batch = video_ids[i:i+50]
            ids_str = ",".join(batch)
            url = f"{self.youtube_base_url}/videos?part=statistics,liveStreamingDetails&id={ids_str}&key={self.api_key}&hl=zh-Hant"

            res = requests.get(url)
            if self._check_quota(res): return stats_map

            if res.status_code == 200:
                data = res.json()
                for item in data.get('items', []):
                    vid = item.get('id')
                    # 檢查是否為未來的首播 (Scheduled Premiere)
                    live_details = item.get('liveStreamingDetails')
                    if live_details:
                        scheduled_start = live_details.get('scheduledStartTime')
                        if scheduled_start:
                            try:
                                # 解析 ISO 8601 時間 (Python 3.7+ fromisoformat 處理 Z 需替換為 +00:00)
                                dt_start = datetime.datetime.fromisoformat(scheduled_start.replace('Z', '+00:00'))
                                # 如果預定時間晚於現在 -> 略過 (不計入 viewCount 統計)
                                if dt_start > now_utc:
                                    stats_map[vid] = None
                                    continue
                            except ValueError:
                                pass # 若時間格式解析失敗，則忽略檢查，照常處理
                    # 取得觀看數
                    view_count = item.get('statistics', {}).get('viewCount')
                    if view_count:
                        stats_map[vid] = int(view_count)
                    else:
                        stats_map[vid] = None # 會員限定或隱藏數據

        return stats_map

    def parse_episode_number(self, title):
        """
        從標題解析集數
        回傳: int 或 float (不減 1，直接回傳原始數值)
        """

        # ==========================================
        # 定義解析規則 (優先順序由上而下)
        # 規則：每個 Regex 必須包含一個 capturing group (...) 用來抓取數字
        # ==========================================
        patterns = [
            # 1. 中文規則 (例如: 第1集, 第 12 話, 第1.5話)
            r'第\s*(\d+(?:\.\d+)?)\s*[集話]',
            # 2. 英文規則 (例如: Episode 5, Episode.05)
            r'Episode\s*[\.]?\s*(\d+(?:\.\d+)?)',
            # 3. 泰文規則 (例如: ตอนที่ 5)
            r'ตอนที่\s*(\d+(?:\.\d+)?)',
            # 4. 越文規則 (例如: Tập 5)
            r'Tập\s*(\d+(?:\.\d+)?)',
            # 5. 印尼文規則 (例如: Misi 5)
            r'Misi\s*(\d+(?:\.\d+)?)',
            # 6. 馬來文規則 (例如: Episod 5)
            r'Episod\s*[\.]?\s*(\d+(?:\.\d+)?)',

            # 0. 簡寫規則 (例如: EP.5, EP 05)
            r'EP\s*[\.]?\s*(\d+(?:\.\d+)?)',
            # 0. 方括號規則 (例如: [12], [05]) - 視情況開啟，可能會誤判年份
            # r'\[(\d+(?:\.\d+)?)\]',
            # 0. 純井號規則 (例如: #12)
            # r'#\s*(\d+(?:\.\d+)?)',
        ]

        # ==========================================
        # 邏輯執行 (通常不需要修改)
        # ==========================================
        for pattern in patterns:
            # re.IGNORECASE 讓 EP/ep/Episode/episode 都能抓到
            match = re.search(pattern, title, re.IGNORECASE)
            if match:
                try:
                    # 抓取第一個群組的數字字串
                    num_str = match.group(1)
                    num = float(num_str)

                    # 如果是整數 (例如 1.0)，轉成 int 回傳
                    if num.is_integer():
                        return int(num)
                    # 否則回傳浮點數 (例如 1.5)
                    return num
                except:
                    # 如果轉換失敗，嘗試下一個規則
                    continue

        # 如果所有規則都沒抓到
        return None

    def is_in_offset(self, title, offset_range=None, offset_string=None):
        """檢查是否在 OFFSET 範圍內"""

        # 1. 檢查字串 Offset (如果有指定)
        if offset_string is not None:
            # 如果標題不包含指定字串，則不在範圍內 -> False
            if offset_string not in title:
                return False

        # 2. 檢查範圍 Offset (如果有指定)
        if offset_range is not None:
            ep_idx = self.parse_episode_number(title)

            # 如果有設定範圍，但標題無法解析出集數 (例如 "特別篇")，視為不在正片範圍內 -> False
            if ep_idx is None:
                return False 

            try:
                start, end = offset_range
                # 檢查是否在集數範圍內
                if not (start <= ep_idx <= end):
                    return False
            except:
                return False

        # 如果通過上述檢查 (或沒有設定對應的檢查)，則回傳 True
        return True

    def is_ignored_keyword(self, title):
        """檢查是否包含忽略關鍵字"""
        for kw in self.ignore_keywords:
            # 如果這個關鍵字有例外，且 title 包含例外名稱，就不忽略
            if kw in self.ignore_keywords_exceptions:
                exceptions = self.ignore_keywords_exceptions[kw]
                if any(exc in title for exc in exceptions):
                    continue
            # 如果 title 包含關鍵字，就忽略
            if kw in title:
                return True
        return False

    def process_anime(self, anime_item):
        """處理單一動畫的邏輯"""
        if self.quota_exceeded: return None

        link_url = anime_item.get('link_url')
        offset_str = anime_item.get('offset')

        # 解析 Offset 字串 "[0,11]" -> [0, 11]
        offset_range = None
        offset_string = None
        if offset_str:
            try:
                offset_range = json.loads(offset_str)
            except:
                offset_string = offset_str
                # pass # 解析失敗則忽略 offset

        playlist_id = self.get_playlist_id(link_url)

        # 初始化回傳值
        result = {
            "avg": "---",
            "total": "---",
            "first": "---",
            "valid_count": 0,       # 初始化有效集數
            "ep_count_update": None # 可選：是否要更新總集數
        }

        if not playlist_id:
            # 沒有連結或連結錯誤，視為下架或無效 (依需求這裡可能不填寫，或是填 ---)
            return None # 不做動作

        # 3. 取得播放清單
        items = self.get_playlist_items(playlist_id)

        if items == "REMOVED":
            return {k: "已下架" for k in result}

        if not items:
            return result # 全空 "---"

        # 4 & 5. 過濾影片
        valid_videos = []
        for item in items:
            title = item['title']

            # 5. 關鍵字過濾
            if self.is_ignored_keyword(title):
                continue

            # 4. Offset 過濾
            if offset_range and not self.is_in_offset(title, offset_range=offset_range):
                continue
            if offset_string and not self.is_in_offset(title, offset_string=offset_string):
                continue

            valid_videos.append(item)

        if not valid_videos:
            return result # 過濾後無影片

        # 6. 取得播放次數
        video_ids = [v['id'] for v in valid_videos]
        stats = self.get_video_stats(video_ids)

        if self.quota_exceeded: return None

        # 計算統計
        total_views = 0
        valid_view_count = 0 # 用於計算平均的分母 (排除會員限定)
        first_ep_views = None

        # 尋找首集 (根據集數最小的，或是列表中的第一個)
        # 為了準確，我們先嘗試解析集數來排序
        sorted_videos = []
        for v in valid_videos:
            ep_idx = self.parse_episode_number(v['title'])
            # 如果無法解析，給一個很大的數放到後面，或保持原序
            sort_key = ep_idx if ep_idx is not None else 999999
            sorted_videos.append({**v, "sort_key": sort_key})

        # 根據集數排序 (由小到大)
        sorted_videos.sort(key=lambda x: x['sort_key'])

        # 計算總量與平均
        has_any_view_data = False

        for v in sorted_videos:
            vid = v['id']
            views = stats.get(vid)

            if views is not None:
                has_any_view_data = True
                total_views += views
                valid_view_count += 1

            # 判斷首集觀看 (排序後的第一個有效影片)
            if first_ep_views is None:
                # 只有當首集有數據時才填寫，如果是會員限定(None)，首集保持 None
                if views is not None:
                    first_ep_views = views
                else:
                    first_ep_views = "---" # 首集是會員限定

        # 如果全部影片都沒有數據 (都是會員限定或錯誤)
        if not has_any_view_data:
            return result # 維持 "---"

        # 計算平均 (無條件捨去取整)
        avg_views = int(total_views / valid_view_count) if valid_view_count > 0 else 0

        # 格式化數字 (依需求，這裡保持 int，寫入時 GAS 會處理，或轉 string)
        result["total"] = total_views
        result["avg"] = avg_views
        # 如果首集觀看還是 None (例如過濾後沒影片，但在前面 valid_videos check 已擋掉)，設為 ---
        result["first"] = first_ep_views if first_ep_views is not None else "---"
        # 回傳有效影片數量 (集數)
        result["valid_count"] = len(valid_videos)

        return result

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
            "text": f"[日本動畫 Youtube亞洲新番觀看量]\n發生錯誤:\n\n{error_msg}"
        }
        requests.post(url, json=payload)
        print("已發送錯誤通知至 Telegram")
    except Exception as e:
        print(f"發送 Telegram 失敗: {e}")

def process_single_sheet(sheet_name):
    """處理單一工作表的完整流程 (讀取 -> 抓取 -> 分區排名 -> 綜合排名 -> 寫入)"""
    print(f"\n====== 準備處理工作表: {sheet_name} ======")

    # 讀取工作表資料
    try:
        sheet_data = current_gas_api.get_sheet_data(sheet_name)
    except Exception as e:
        print(f"讀取工作表 {sheet_name} 失敗: {e}")
        return

    if not sheet_data:
        print(f"工作表 {sheet_name} 無資料或格式錯誤，跳過。")
        return

    updates_batch = []
    row_max_ep_map = {}
    global_avg_sum_map = {}         # 用於計算綜合排名的累加器 { row_idx: total_sum_avg }
    global_comp_rank_cell_map = {}  # 記錄每一行對應的儲存格位置 (只需記一次) { row_idx: cell_address }
    
    # 遍歷區域
    for region_name, anime_list in sheet_data.items():
        print(f"\n[{sheet_name}] 正在處理區域: {region_name}")

        # 該區域暫存清單
        region_results = []

        # --- 第一階段：讀取該區域所有影片數據 ---
        for anime in anime_list:
            if yt_processor.quota_exceeded:
                print("!!! 配額已滿，停止處理 !!!")
                return # 直接結束函式

            # 記錄綜合排名欄位位置 (如果有的話)
            row_idx = int(anime['name_cell'][1:])
            if anime.get('comp_rank_cell'):
                global_comp_rank_cell_map[row_idx] = anime['comp_rank_cell']

            name = anime['anime_name']
            print(f"  > 處理: {name.split("\n")[0]} ...", end=" ", flush=True)

            stats = yt_processor.process_anime(anime)

            if stats is None:
                print("跳過 (無連結/配額滿/API錯誤)")
                region_results.append({"anime": anime, "stats": None})
                continue

            print(f"總={stats['total']}, 均={stats['avg']}, 首={stats['first']}")
            region_results.append({"anime": anime, "stats": stats})

            # 累加平均流量至全域 Map
            # 只有當 avg 是有效整數時才累加
            if isinstance(stats.get('avg'), int):
                current_sum = global_avg_sum_map.get(row_idx, 0)
                global_avg_sum_map[row_idx] = current_sum + stats['avg']

        # --- 第二階段：計算該區域排名 ---
        valid_for_ranking = []
        for item in region_results:
            stats = item['stats']
            # 只有當 avg 是有效整數時才參與排名
            if stats and isinstance(stats.get('avg'), int):
                valid_for_ranking.append(item)

        # 排序 (大到小)
        valid_for_ranking.sort(key=lambda x: x['stats']['avg'], reverse=True)

        # 建立排名表
        rank_map = {}
        for i, item in enumerate(valid_for_ranking):
            row_idx = int(item['anime']['name_cell'][1:])
            rank_map[row_idx] = i + 1

        # --- 第三階段：準備寫入資料 ---
        for item in region_results:
            anime = item['anime']
            stats = item['stats']
            row_idx = int(anime['name_cell'][1:])
            row_updates = []

            # 1. 處理排名
            if anime['rank_cell']:
                if row_idx in rank_map:
                    row_updates.append({"cell": anime['rank_cell'], "value": rank_map[row_idx]})
                else:
                    # 數值無效則清除排名
                    row_updates.append({"cell": anime['rank_cell'], "value": ""})

            # 如果 stats 為空，僅更新排名後跳過
            if not stats:
                if row_updates:
                    updates_batch.append({
                        "row_check_name": anime['anime_name'],
                        "target_row": row_idx,
                        "updates": row_updates
                    })
                continue

            # 2. 處理觀看數據
            if anime['avg_view_cell']:
                row_updates.append({"cell": anime['avg_view_cell'], "value": stats.get('avg', "---")})
            if anime['total_view_cell']:
                row_updates.append({"cell": anime['total_view_cell'], "value": stats.get('total', "---")})
            if anime['first_view_cell']:
                row_updates.append({"cell": anime['first_view_cell'], "value": stats.get('first', "---")})

            # 3. 處理總集數
            current_max_ep = row_max_ep_map.get(row_idx, 0)
            this_region_ep_count = stats.get('valid_count', 0)

            try:
                if this_region_ep_count > current_max_ep:
                    row_max_ep_map[row_idx] = this_region_ep_count
                    if anime['ep_count_cell']:
                        row_updates.append({"cell": anime['ep_count_cell'], "value": this_region_ep_count})
            except Exception as e:
                pass

            if row_updates:
                updates_batch.append({
                    "row_check_name": anime['anime_name'],
                    "target_row": row_idx,
                    "updates": row_updates
                })

    # ==========================================
    # 第四階段：計算並寫入「綜合排名」
    # ==========================================
    if not yt_processor.quota_exceeded and global_comp_rank_cell_map:
        print(f"\n[{sheet_name}] 正在計算綜合排名...")

        # 1. 將累加結果轉為列表並排序 [(row_idx, sum_avg), ...]
        # 過濾掉總和為 0 的 (視需求，如果全區都沒觀看量，是否要排名？這裡假設有值才排)
        valid_global_list = [
            (row, val) for row, val in global_avg_sum_map.items() if val > 0
        ]

        # 排序：流量大 -> 小
        valid_global_list.sort(key=lambda x: x[1], reverse=True)

        # 2. 建立排名 Map { row_idx: rank }
        global_rank_map = {row: i+1 for i, (row, val) in enumerate(valid_global_list)}

        # 3. 產生寫入更新
        # 這裡我們需要一個參考名稱來通過 row_check_name 檢查
        # 由於 updates_batch 結構需要 name，我們得從 sheet_data 裡隨便找一個區域把 name 撈出來
        # 比較簡單的方法是：利用 global_comp_rank_cell_map 的 key (row_idx) 反查名稱
        # 但為了效率，我們直接遍歷 sheet_data 的第一個區域來對應 Row 和 Name

        first_region_list = list(sheet_data.values())[0]
        row_to_name_map = {int(a['name_cell'][1:]): a['anime_name'] for a in first_region_list}

        for row_idx, cell_addr in global_comp_rank_cell_map.items():
            anime_name = row_to_name_map.get(row_idx)
            if not anime_name: continue

            updates = []
            if row_idx in global_rank_map:
                updates.append({"cell": cell_addr, "value": global_rank_map[row_idx]})
            else:
                # 沒在排名內 (可能是流量為0或無數據)，清空
                updates.append({"cell": cell_addr, "value": ""})

            updates_batch.append({
                "row_check_name": anime_name,
                "target_row": row_idx,
                "updates": updates
            })

    # 5. 批量寫入該工作表
    if updates_batch and not yt_processor.quota_exceeded:
        print(f"\n[寫入] 準備寫入 {len(updates_batch)} 筆資料至 {sheet_name}...")
        try:
            resp = current_gas_api.batch_update(sheet_name, updates_batch)
            print(f"[寫入] 完成! 更新數: {resp.get('updated')}, 錯誤: {resp.get('errors')}")
            print(f"[排序] 正在對 {sheet_name} 進行綜合排名排序...")
            sort_resp = current_gas_api.sort_data(sheet_name)
            print(f"[排序] {sort_resp.get('message')}")
        except Exception as e:
            print(f"[寫入/排序] 失敗: {e}")
    elif yt_processor.quota_exceeded:
        print("\n[暫停] 因為配額已滿，不執行寫入")
    else:
        print(f"\n[資訊] 工作表 {sheet_name} 沒有需要更新的資料")

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
            if ss_id == "":
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
                raise e # 拋出錯誤以便觸發 TG 通知

            # 2. 決定要更新哪些工作表
            sheets_to_process = set()
            if "0" in schedule:
                sheets_to_process.update(schedule["0"])
            if today_key in schedule:
                sheets_to_process.update(schedule[today_key])
                
            target_sheets = sorted(list(sheets_to_process))

            if not target_sheets:
                print("此試算表今天沒有安排需要更新的工作表。")
                continue # 繼續下一個試算表 (不視為錯誤)

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
        
        # 取得詳細錯誤訊息
        import traceback, sys
        error_class = e.__class__.__name__
        detail = e.args[0]
        cl, exc, tb = sys.exc_info()
        lastCallStack = traceback.extract_tb(tb)[-1]
        fileName = lastCallStack[0]
        lineNum = lastCallStack[1]
        funcName = lastCallStack[2]
        errMsg = "File \"{}\", line {}, in {}: [{}] {}".format(fileName, lineNum, funcName, error_class, detail)
        print(f"\n❌ 發生嚴重錯誤: {errMsg}")
        send_telegram_error(errMsg)

    # 程式結束前的動作
    if not global_error_occurred:
        print("\n✅ 所有任務執行完成，且無錯誤。")
        
        # 更新所有表格的系統時間
        try:
            now_str = datetime.datetime.now(TZ_UTC_PLUS_8).strftime("%Y年%m月%d日 %H:%M")
            print(f"正在更新系統時間標記: {now_str}")
            
            for ss_id in SPREADSHEET_IDS:
                if ss_id == "":
                    continue
                api = AnimeAPI(GAS_URL, ss_id)
                res = api.update_system_time(now_str)
                print(f"  - 試算表 {ss_id[-4:]}... : {res.get('message')}")
        
        except Exception as e:
             print(f"更新系統時間時發生錯誤: {e}")
             # 這裡錯誤是否要發 TG？依需求，通常這裡失敗不算主流程失敗，但如果要嚴格執行：
             send_telegram_error(f"更新系統時間失敗: {e}")
             return

        # 發送 Heartbeat
        try:
            print("正在發送 Heartbeat...")
            requests.get(HEARTBEAT_URL)
            print("Heartbeat 發送成功")
        except Exception as e:
            print(f"Heartbeat 發送失敗: {e}")

if __name__ == "__main__":
    main()
