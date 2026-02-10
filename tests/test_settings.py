from algo.config import settings

def test_settings_exist():
    assert settings.data_dir.name == "data"
    assert settings.artifacts_dir.name == "artifacts"
