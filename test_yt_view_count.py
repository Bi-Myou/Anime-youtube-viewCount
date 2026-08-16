import csv
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import yt_view_count as mod


class StubYouTubeDataProcessor(mod.YouTubeDataProcessor):
    def __init__(self, playlist_items_map=None, stats_map=None):
        super().__init__("test-key")
        self.playlist_items_map = playlist_items_map or {}
        self.stats_map = stats_map or {}

    def get_playlist_items(self, playlist_id, playlist_sequence=0):
        items = self.playlist_items_map.get(playlist_id, [])
        if items == "REMOVED":
            return "REMOVED"
        return [
            {
                **item,
                "playlist_id": playlist_id,
                "playlist_sequence": playlist_sequence,
            }
            for item in items
        ]

    def get_video_stats(self, video_ids):
        return {video_id: self.stats_map[video_id] for video_id in video_ids if video_id in self.stats_map}


class YtViewCountTests(unittest.TestCase):
    def test_ignore_keyword_requires_token_boundary_for_short_acronyms(self):
        processor = mod.YouTubeDataProcessor("test-key")
        rule = processor.parse_offset_rule("TVSP")

        self.assertTrue(
            processor.should_include_video(
                "I Got a CHEAT SKILL in ANOTHER WORLD and Became UNRIVALED "
                "in the REAL WORLD, Too - TVSP",
                rule,
            )
        )
        self.assertFalse(processor.should_include_video("Anime ED - TVSP", rule))
        self.assertFalse(processor.should_include_video("Anime OP1 - TVSP", rule))

    @patch("yt_view_count.requests.get")
    def test_video_stats_excludes_missing_and_future_videos_from_result(self, mock_get):
        response = Mock(status_code=200)
        response.json.return_value = {
            "items": [
                {"id": "released", "statistics": {"viewCount": "123"}},
                {"id": "no-view-count", "statistics": {}},
                {
                    "id": "future",
                    "statistics": {"viewCount": "0"},
                    "liveStreamingDetails": {"scheduledStartTime": "2999-01-01T00:00:00Z"},
                },
            ]
        }
        mock_get.return_value = response
        processor = mod.YouTubeDataProcessor("test-key")

        video_ids = ["released", "no-view-count", "future", "hidden"]
        stats = processor.get_video_stats(video_ids)
        cached_stats = processor.get_video_stats(video_ids)

        self.assertEqual(stats, {"released": 123, "no-view-count": None})
        self.assertEqual(cached_stats, stats)
        mock_get.assert_called_once()

    def test_parse_sheet_snapshot_uses_sample_region_layout(self):
        csv_path = Path("日本動畫 Youtube亞洲新番觀看量｜木棉花 - 2026.04.csv")
        with csv_path.open("r", encoding="utf-8-sig", newline="") as fh:
            rows = list(csv.reader(fh))

        snapshot = {
            "row1": rows[0],
            "row2": rows[1],
            "rows": [
                {
                    "row_num": 3,
                    "values": rows[2],
                    "row_signature": "sig-1",
                    "link_urls_by_col": {"9": ["https://www.youtube.com/playlist?list=TW"]},
                }
            ],
        }

        model = mod.parse_sheet_snapshot("2026.04", snapshot)

        self.assertEqual(
            [region.name for region in model.regions],
            [
                "臺灣｜TW",
                "香港、澳門｜HK、MO",
                "亞洲｜ASIA",
                "印尼｜ID",
                "汶萊、馬來西亞｜BN、MY",
                "菲律賓｜PH",
                "泰國｜TH",
                "越南｜VN",
                "印度｜IN",
            ],
        )
        self.assertEqual(model.rows[0].regions["臺灣｜TW"].definition.offset_cols, [10])
        self.assertEqual(model.rows[0].regions["印度｜IN"].definition.offset_cols, [58])
        self.assertEqual(model.rows[0].ep_count_cell, "D3")

    def test_parse_sheet_snapshot_detects_dynamic_leading_columns_and_multiple_offsets(self):
        csv_path = Path("日本動畫 Youtube亞洲新番觀看量｜回歸線+曼迪 - 2026.04.csv")
        with csv_path.open("r", encoding="utf-8-sig", newline="") as fh:
            rows = list(csv.reader(fh))

        snapshot = {
            "row1": rows[0],
            "row2": rows[1],
            "rows": [
                {
                    "row_num": 3,
                    "values": rows[2],
                    "row_signature": "sig-rg-1",
                    "link_urls_by_col": {"10": ["https://www.youtube.com/playlist?list=RG"]},
                },
                {
                    "row_num": 4,
                    "values": rows[3][:10] + ["", '{"match":"晚上架"}', rows[3][12]],
                    "row_signature": "sig-rg-2",
                    "link_urls_by_col": {"10": ["https://www.youtube.com/playlist?list=MD"]},
                },
            ],
        }

        model = mod.parse_sheet_snapshot("2026.04", snapshot)

        self.assertEqual(model.name_col, 1)
        self.assertEqual(model.comp_rank_col, 4)
        self.assertEqual(model.ep_count_col, 5)
        self.assertEqual([region.name for region in model.regions], ["中文區｜TW+HK+MO"])
        self.assertEqual(model.regions[0].start_col, 6)
        self.assertEqual(model.regions[0].offset_cols, [11, 12])
        self.assertEqual(model.rows[0].ep_count_cell, "E3")
        self.assertEqual(model.rows[0].regions["中文區｜TW+HK+MO"].offset_cell, "K3")
        self.assertEqual(model.rows[1].regions["中文區｜TW+HK+MO"].offset_cell, "L4")
        self.assertEqual(model.rows[1].regions["中文區｜TW+HK+MO"].offset_raw, '{"match":"晚上架"}')

    def test_calculate_sheet_updates_marks_invalid_offset_json_and_keeps_signatures(self):
        snapshot = {
            "row1": ["作品名稱", "日本窗口", "綜合排名\n（平均流量最多）", "總集數", "臺灣｜TW", "", "", "", "", "", "香港、澳門｜HK、MO", "", "", "", "", ""],
            "row2": ["", "", "", "", "排名", "平均觀看", "總觀看量", "首集觀看", "連結", "OFFSET", "排名", "平均觀看", "總觀看量", "首集觀看", "連結", "OFFSET"],
            "rows": [
                {
                    "row_num": 3,
                    "values": [
                        "作品A",
                        "窗口A",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "【連結】",
                        "{bad json}",
                        "",
                        "",
                        "",
                        "",
                        "【連結】",
                        '{"offset":[2,2]}',
                    ],
                    "row_signature": "sig-3",
                    "link_urls_by_col": {
                        "9": ["https://www.youtube.com/playlist?list=PLTW"],
                        "15": ["https://www.youtube.com/playlist?list=PLHK"],
                    },
                }
            ],
        }

        processor = StubYouTubeDataProcessor(
            playlist_items_map={
                "PLTW": [
                    {"id": "tw-1", "title": "Episode 1", "position": 0},
                    {"id": "tw-2", "title": "Episode 2", "position": 1},
                ],
                "PLHK": [
                    {"id": "hk-1", "title": "Episode 1", "position": 0},
                    {"id": "hk-2", "title": "Episode 2", "position": 1},
                ],
            },
            stats_map={"tw-1": 100, "tw-2": 50, "hk-1": 200, "hk-2": 300},
        )

        model = mod.parse_sheet_snapshot("2026.04", snapshot)
        updates = mod.calculate_sheet_updates(model, processor)

        self.assertEqual(len(updates), 1)
        update = updates[0]
        self.assertEqual(update["expected_row_signature"], "sig-3")

        format_map = {item["cell"]: item["font_color"] for item in update["format_updates"]}
        self.assertEqual(format_map["J3"], mod.ERROR_FONT_COLOR)
        self.assertEqual(format_map["P3"], mod.DEFAULT_FONT_COLOR)

    def test_process_region_uses_region_specific_offset_rules(self):
        processor = StubYouTubeDataProcessor(
            playlist_items_map={
                "PLAYLIST": [
                    {"id": "ep1", "title": "Episode 1", "position": 0},
                    {"id": "ep2", "title": "Episode 2", "position": 1},
                ]
            },
            stats_map={"ep1": 100, "ep2": 200},
        )

        region = mod.RegionDefinition("臺灣｜TW", 5, 5, 6, 7, 8, 9, 10)
        region_data = mod.RegionRowData(
            definition=region,
            row_num=3,
            anime_name="作品A",
            link_urls=["https://www.youtube.com/playlist?list=PLAYLIST"],
            offset_raw=None,
            offset_col=10,
        )

        ep1_only = processor.process_region(region_data, processor.parse_offset_rule("[1,1]"))
        ep2_only = processor.process_region(region_data, processor.parse_offset_rule('{"offset":[2,2]}'))

        self.assertEqual(ep1_only.total, 100)
        self.assertEqual(ep1_only.first, 100)
        self.assertEqual(ep2_only.total, 200)
        self.assertEqual(ep2_only.first, 200)

    def test_offset_match_requires_all_keywords_in_array(self):
        processor = StubYouTubeDataProcessor(
            playlist_items_map={
                "PLAYLIST": [
                    {"id": "both", "title": "Episode 1 日語原音版", "position": 0},
                    {"id": "jp", "title": "Episode 2 日語版", "position": 1},
                    {"id": "orig", "title": "Episode 3 原音版", "position": 2},
                    {"id": "dub", "title": "Episode 3 中文配音版", "position": 2},
                ]
            },
            stats_map={"both": 100, "jp": 200, "orig": 300, "dub": 400},
        )

        region = mod.RegionDefinition("臺灣｜TW", 5, 5, 6, 7, 8, 9, 10)
        region_data = mod.RegionRowData(
            definition=region,
            row_num=3,
            anime_name="作品A",
            link_urls=["https://www.youtube.com/playlist?list=PLAYLIST"],
            offset_raw=None,
            offset_col=10,
        )

        result = processor.process_region(
            region_data,
            processor.parse_offset_rule('{"match":["日語","原音"]}'),
        )

        self.assertEqual(result.valid_count, 1)
        self.assertEqual(result.total, 100)
        self.assertEqual(result.first, 100)

    def test_process_region_prefers_smallest_episode_for_descending_playlist(self):
        processor = StubYouTubeDataProcessor(
            playlist_items_map={
                "DESC": [
                    {"id": "ep5", "title": "Episode 5", "position": 0},
                    {"id": "ep4", "title": "Episode 4", "position": 1},
                    {"id": "ep3", "title": "Episode 3", "position": 2},
                    {"id": "ep2", "title": "Episode 2", "position": 3},
                    {"id": "ep1", "title": "Episode 1", "position": 4},
                ]
            },
            stats_map={"ep1": 101, "ep2": 102, "ep3": 103, "ep4": 104, "ep5": 105},
        )

        region = mod.RegionDefinition("臺灣｜TW", 5, 5, 6, 7, 8, 9, 10)
        region_data = mod.RegionRowData(
            definition=region,
            row_num=3,
            anime_name="作品A",
            link_urls=["https://www.youtube.com/playlist?list=DESC"],
            offset_raw=None,
            offset_col=10,
        )

        result = processor.process_region(region_data, processor.parse_offset_rule(""))
        self.assertEqual(result.first, 101)
        self.assertEqual(result.valid_count, 5)

    def test_process_region_falls_back_to_playlist_order_when_episode_missing(self):
        processor = StubYouTubeDataProcessor(
            playlist_items_map={
                "DESC": [
                    {"id": "a", "title": "Main Story Alpha", "position": 0},
                    {"id": "b", "title": "Main Story Beta", "position": 1},
                    {"id": "c", "title": "Main Story Gamma", "position": 2},
                ]
            },
            stats_map={"a": 10, "b": 20, "c": 30},
        )

        region = mod.RegionDefinition("臺灣｜TW", 5, 5, 6, 7, 8, 9, 10)
        region_data = mod.RegionRowData(
            definition=region,
            row_num=3,
            anime_name="作品A",
            link_urls=["https://www.youtube.com/playlist?list=DESC"],
            offset_raw=None,
            offset_col=10,
        )

        result = processor.process_region(region_data, processor.parse_offset_rule('{"playlist_order":"desc"}'))
        self.assertEqual(result.first, 30)


if __name__ == "__main__":
    unittest.main()
