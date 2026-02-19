from pathlib import Path

import yaml
from pydantic import BaseModel, Field, PrivateAttr


# an asset itself
class Asset(BaseModel):
    key: str
    kind: str
    name: str
    identifiers: dict[str, str] = Field(default_factory=dict)


# a collection of assets
class AssetFile(BaseModel):
    assets: list[Asset]

    _by_key: dict[str, Asset] = PrivateAttr(default_factory=dict)

    # after yaml is loaded we build index
    def model_post_init(self, __context) -> None:
        self._by_key = {a.key: a for a in self.assets}

    def get(self, key: str) -> Asset:
        if key not in self._by_key:
            raise KeyError(f"Unknown asset key: {key}")
        return self._by_key[key]


_REGISTRY: AssetFile | None = None


def _assets_yaml_path() -> Path:
    return Path(__file__).with_name("assets.yaml")


def load_registry() -> AssetFile:
    path = _assets_yaml_path()
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return AssetFile.model_validate(data)


def get_registry() -> AssetFile:
    global _REGISTRY
    if _REGISTRY is None:
        _REGISTRY = load_registry()
    return _REGISTRY


def get_asset(key: str) -> Asset:
    return get_registry().get(key)


def get_identifier(key: str, identifier: str) -> str:
    asset = get_asset(key)
    if identifier not in asset.identifiers:
        available = ", ".join(sorted(asset.identifiers.keys()))
        raise KeyError(f"Asset '{key}' has no identifier '{identifier}'. Available: {available}")
    return asset.identifiers[identifier]


def has_identifier(key: str, identifier: str) -> bool:
    asset = get_asset(key)
    return identifier in asset.identifiers


def list_asset_keys() -> list[str]:
    reg = get_registry()
    return [a.key for a in reg.assets]

def list_asset_keys_by_kind(kinds: set[str]) -> list[str]:
    reg = get_registry()
    return [a.key for a in reg.assets if a.kind in kinds]