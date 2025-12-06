import pytest

from program.services.scrapers.shared import select_top_n
from program.settings.models import (
    PathProfileMapping,
    RankingProfileSettings,
    RankingSettings,
)


def test_get_profile_for_path_longest_prefix():
    settings = RankingSettings(
        default_profile="default",
        keep_versions_per_item=2,
        profiles={
            "default": RankingProfileSettings(),
            "hq": RankingProfileSettings(),
            "mobile": RankingProfileSettings(),
        },
        path_profiles=[
            PathProfileMapping(path="/mnt", profile_name="hq"),
            PathProfileMapping(path="/mnt/debrid", profile_name="mobile"),
            PathProfileMapping(path="/mnt/debrid/riven", profile_name="hq"),
        ],
    )

    assert settings.get_profile_name_for_path("/mnt/debrid/riven/mobile") == "hq"
    assert settings.get_profile_name_for_path("/mnt/debrid/other") == "mobile"
    assert settings.get_profile_name_for_path("/unknown") == "default"


def test_legacy_ranking_settings_wrapped():
    legacy = {"languages": {"exclude": []}, "options": {"remove_all_trash": True}}
    settings = RankingSettings.model_validate(legacy)
    assert settings.default_profile == "default"
    assert "default" in settings.profiles
    assert settings.keep_versions_per_item == 1


def test_select_top_n_trims_order():
    streams = {"a": object(), "b": object(), "c": object()}
    trimmed = select_top_n(streams, 2)
    assert list(trimmed.keys()) == ["a", "b"]

    trimmed_zero = select_top_n(streams, 0)
    assert trimmed_zero == {}


def test_unknown_profile_mapping_falls_back_to_default():
    settings = RankingSettings(
        default_profile="default",
        profiles={"default": RankingProfileSettings()},
        path_profiles=[PathProfileMapping(path="/mnt/debrid", profile_name="missing")],
    )

    assert settings.get_profile_name_for_path("/mnt/debrid/riven") == "default"


def test_keep_versions_profile_override_and_global():
    settings = RankingSettings(
        keep_versions_per_item=3,
        profiles={
            "default": RankingProfileSettings(),
            "mobile": RankingProfileSettings(keep_versions_per_item=1),
        },
    )

    assert settings.get_keep_versions_for_profile("mobile") == 1
    assert settings.get_keep_versions_for_profile("default") == 3
    assert settings.get_keep_versions_for_profile("unknown") == 3
