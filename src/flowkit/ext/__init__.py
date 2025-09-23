from .registry import PluginRegistry, StorageProvider

PLUGIN_REGISTRY = PluginRegistry()

__all__ = [
    "PLUGIN_REGISTRY",
    "PluginRegistry",
    "StorageProvider",
]
