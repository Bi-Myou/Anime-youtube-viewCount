import unittest
import json
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
    def make_region_data(self, urls):
        return mod.RegionRowData(
            definition=mod.RegionDefinition("臺灣｜TW", 5, 5, 6, 7, 8, 9, [10]),
            row_num=3,
            anime_name="作品A",
            link_urls=urls,
            offset_raw=None,
            offset_col=10,
        )

    def test_show_links_resolve_to_playlist_id(self):
        processor = mod.YouTubeDataProcessor("test-key")
        for url in [
            "https://www.youtube.com/show/VLPLcKfVhnYS2Nk?sbp=KgtNZGlhZ2x1VVhQUUAB",
            "https://www.youtube.com/show/VLPLcKfVhnYS2Nk",
            "https://m.youtube.com/show/VLPLcKfVhnYS2Nk/",
            "https://www.youtube.com/playlist?list=PLcKfVhnYS2Nk",
        ]:
            with self.subTest(url=url):
                self.assertEqual(processor.get_playlist_id(url), "PLcKfVhnYS2Nk")
        for url in [
            "https://www.youtube.com/show/VL",
            "https://www.youtube.com/show/PLcKfVhnYS2Nk",
            "https://example.com/show/VLPLcKfVhnYS2Nk",
        ]:
            with self.subTest(url=url):
                self.assertIsNone(processor.get_playlist_id(url))

    def test_show_link_counts_playlist_and_deduplicates_equivalent_link(self):
        processor = StubYouTubeDataProcessor(
            {"PLcKfVhnYS2Nk": [
                {"id": "ep2", "title": "Episode 2", "position": 0},
                {"id": "ep1", "title": "Episode 1", "position": 1},
            ]},
            {"ep1": 100, "ep2": 200},
        )
        region_data = self.make_region_data([
            "https://www.youtube.com/show/VLPLcKfVhnYS2Nk?sbp=KgtNZGlhZ2x1VVhQUUAB",
            "https://www.youtube.com/playlist?list=PLcKfVhnYS2Nk",
        ])
        with patch.object(processor, "get_playlist_items", wraps=processor.get_playlist_items) as fetch:
            result = processor.process_region(region_data, processor.parse_offset_rule(""))
            fetch.assert_called_once_with("PLcKfVhnYS2Nk", 0)
        self.assertEqual(result, mod.RegionStats(avg=150, total=300, first=100, valid_count=2))
        self.assertEqual(processor.duplicate_playlist_refs_skipped, 1)

    @patch("yt_view_count.requests.get")
    def test_direct_video_urls_have_one_episode_and_identical_view_counts(self, mock_get):
        video_id = "Abc_123-xYz"
        response = Mock(status_code=200)
        response.json.return_value = {
            "items": [{"id": video_id, "statistics": {"viewCount": "123"}}]
        }
        mock_get.return_value = response
        urls = [
            f"https://www.youtube.com/watch?v={video_id}",
            f"https://www.youtube.com/watch?v={video_id}&list=IGNORED&index=2",
            f"https://youtu.be/{video_id}?si=share",
            f"https://www.youtube.com/shorts/{video_id}",
            f"https://www.youtube.com/live/{video_id}",
            f"https://www.youtube.com/embed/{video_id}",
            f"https://m.youtube.com/watch?v={video_id}",
        ]
        for url in urls:
            with self.subTest(url=url):
                mock_get.reset_mock()
                processor = mod.YouTubeDataProcessor("test-key")
                result = processor.process_region(
                    self.make_region_data([url]),
                    processor.parse_offset_rule('{"match":"日語","offset":[2,12]}'),
                )
                self.assertEqual(result, mod.RegionStats(avg=123, total=123, first=123, valid_count=1))
                mock_get.assert_called_once()
                self.assertTrue(mock_get.call_args.args[0].endswith("/videos"))
                self.assertEqual(mock_get.call_args.kwargs["params"]["id"], video_id)

    def test_direct_video_missing_or_unavailable_views(self):
        video_id = "Abc_123-xYz"
        for stats, count in [({}, 0), ({video_id: None}, 1), ({video_id: 0}, 1)]:
            with self.subTest(stats=stats):
                processor = StubYouTubeDataProcessor(stats_map=stats)
                result = processor.process_region(
                    self.make_region_data([f"https://youtu.be/{video_id}"]),
                    processor.parse_offset_rule(""),
                )
                self.assertEqual(result.valid_count, count)
                expected = 0 if stats.get(video_id) == 0 else "---"
                self.assertEqual((result.avg, result.total, result.first), (expected,) * 3)

    def test_direct_video_duplicates_and_playlist_overlap_count_once(self):
        video_id = "Abc_123-xYz"
        processor = StubYouTubeDataProcessor(
            {"PL": [{"id": video_id, "title": "Episode 1", "position": 0},
                    {"id": "ep2", "title": "Episode 2", "position": 1}]},
            {video_id: 100, "ep2": 200},
        )
        result = processor.process_region(
            self.make_region_data([
                f"https://youtu.be/{video_id}", f"https://www.youtube.com/watch?v={video_id}",
                "https://www.youtube.com/playlist?list=PL",
            ]),
            processor.parse_offset_rule(""),
        )
        self.assertEqual(result.valid_count, 2)
        self.assertEqual(result.total, 300)
        self.assertEqual(result.first, 100)

    def test_episode_number_formats_and_unrelated_numbers(self):
        processor = mod.YouTubeDataProcessor("test-key")
        for title, expected in [
            ("The Forsaken Saintess and Her Foodie Road Trip in Another World|E1(ENSub/JPDub)", 1),
            ("The Forsaken Saintess and Her Foodie Road Trip in Another World Ep. #12 (ENSub/JPDub)", 12),
            ("作品【01】副標題", 1), ("作品 [01]", 1), ("作品 - 01", 1),
            ("作品｜０１｜副標題", 1), ("作品 第1.5話", 1.5),
            ("作品 [2026]", None), ("作品 Season 2", None),
            ("TITLE1", None), ("作品 - 2026", None),
        ]:
            with self.subTest(title=title):
                self.assertEqual(processor.parse_episode_number(title), expected)

    def test_mixed_anione_titles_select_first_episode_anywhere(self):
        title = "The Forsaken Saintess and Her Foodie Road Trip in Another World"
        for order in [(12, 10, 2, 1), (12, 1, 2, 10)]:
            with self.subTest(order=order):
                items = [
                    {"id": f"ep{ep}", "position": pos,
                     "title": f"{title} Ep. #{ep} (ENSub/JPDub)" if ep >= 10
                     else f"{title}|E{ep}(ENSub/JPDub) 【Ani-One Philippines】"}
                    for pos, ep in enumerate(order)
                ]
                processor = StubYouTubeDataProcessor(
                    {"PL": items}, {f"ep{ep}": ep * 100 for ep in order}
                )
                result = processor.process_region(
                    self.make_region_data(["https://www.youtube.com/playlist?list=PL"]),
                    processor.parse_offset_rule(""),
                )
                self.assertEqual(result.first, 100)
                self.assertEqual(result.total, 2500)
                self.assertEqual(result.avg, 625)
                self.assertEqual(result.valid_count, 4)

    def test_manual_first_episode_selection_preserves_totals_and_filters(self):
        processor = StubYouTubeDataProcessor(
            {"PL": [
                {"id": "a", "title": "Episode 1 序章 日語", "position": 0},
                {"id": "b", "title": "Episode 2 旅程 日語", "position": 1},
                {"id": "c", "title": "Episode 3 旅程 配音", "position": 2},
            ]},
            {"a": 100, "b": 200, "c": 300},
        )
        for payload, expected_first, expected_total, expected_count in [
            ({"first_match": "序章"}, 100, 600, 3),
            ({"first_match": ["旅程", "日語"]}, 200, 600, 3),
            ({"first_video_id": "c"}, 300, 600, 3),
            ({"first_video_id": "c", "first_match": "序章"}, 300, 600, 3),
            ({"first_video_id": "missing"}, "---", 600, 3),
            ({"first_match": "missing"}, "---", 600, 3),
            ({"first_match": "旅程"}, "---", 600, 3),
            ({"first_video_id": "c", "match": "日語"}, "---", 300, 2),
            ({"first_video_id": "a", "offset": [2, 3]}, "---", 500, 2),
        ]:
            with self.subTest(payload=payload):
                result = processor.process_region(
                    self.make_region_data(["https://www.youtube.com/playlist?list=PL"]),
                    processor.parse_offset_rule(json.dumps(payload)),
                )
                self.assertEqual(result.first, expected_first)
                self.assertEqual(result.total, expected_total)
                self.assertEqual(result.valid_count, expected_count)

    def test_sheet_updates_use_direct_video_and_region_first_override(self):
        video_id = "Abc_123-xYz"
        snapshot = {
            "row1": ["作品名稱", "總集數", "臺灣｜TW", "", "", "", "", ""],
            "row2": ["", "", "排名", "平均觀看", "總觀看量", "首集觀看", "連結", "OFFSET"],
            "rows": [
                {"row_num": 3, "values": ["電影", "", "", "", "", "", "連結", ""],
                 "row_signature": "movie", "link_urls_by_col": {"7": [f"https://youtu.be/{video_id}"]}},
                {"row_num": 4,
                 "values": ["影集", "", "", "", "", "", "連結", '{"first_video_id":"b"}'],
                 "row_signature": "series",
                 "link_urls_by_col": {"7": ["https://www.youtube.com/playlist?list=PL"]}},
            ],
        }
        processor = StubYouTubeDataProcessor(
            {"PL": [{"id": "a", "title": "Episode 1", "position": 0},
                    {"id": "b", "title": "Episode 2", "position": 1}]},
            {video_id: 123, "a": 100, "b": 200},
        )
        updates = mod.calculate_sheet_updates(mod.parse_sheet_snapshot("test", snapshot), processor)
        values = {item["cell"]: item["value"] for row in updates for item in row["value_updates"]}
        self.assertEqual([values[cell] for cell in ["B3", "D3", "E3", "F3"]], [1, 123, 123, 123])
        self.assertEqual([values[cell] for cell in ["B4", "D4", "E4", "F4"]], [2, 150, 300, 200])

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
        regions = [
            "臺灣｜TW", "香港、澳門｜HK、MO", "亞洲｜ASIA", "印尼｜ID",
            "汶萊、馬來西亞｜BN、MY", "菲律賓｜PH", "泰國｜TH", "越南｜VN", "印度｜IN",
        ]
        row1 = ["作品名稱", "日本窗口", "綜合排名", "總集數"]
        row2 = [""] * 4
        for region in regions:
            row1.extend([region, "", "", "", "", ""])
            row2.extend(["排名", "平均觀看", "總觀看量", "首集觀看", "連結", "OFFSET"])
        rows = [row1, row2, ["作品A"] + [""] * (len(row1) - 1)]

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
        rows = [
            ["作品名稱", "日本窗口", "代理商", "綜合排名", "總集數", "中文區｜TW+HK+MO",
             "", "", "", "", "", "", "備註"],
            ["", "", "", "", "", "排名", "平均觀看", "總觀看量", "首集觀看", "連結",
             "OFFSET", "OFFSET", ""],
            ["作品A"] + [""] * 12,
            ["作品B"] + [""] * 12,
        ]

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
