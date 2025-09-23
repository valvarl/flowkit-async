# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Central plugin registry:
- Adapters: sources / sinks (outputs) / transforms
- Externals providers
- Hook actions
- Handlers
- Storage providers (by DSN scheme)

Supports explicit registration and entry-point discovery.

Entry points:
  - "flowkit.plugins"            → classes implementing api.adapters.Plugin
  - "flowkit.storage_providers"  → provider objects or factories (one per scheme)
"""

from typing import Any, NamedTuple, Protocol

from flowkit.storage.base.capabilities import StorageCapabilities

from .adapters import Plugin, SinkAdapter, SourceAdapter, TransformOp
from .errors import FlowkitError
from .handlers import BaseHandler

# ───────────────────────────────────────── Types ─────────────────────────────────────────


class RegistryError(FlowkitError):
    """Raised when registration/lookup fails (duplicate/unknown/capability issues)."""


class ProviderFactory(Protocol):
    async def __call__(self, *, dsn: str, **kwargs: Any) -> Any:
        """
        Construct a provider-specific StorageFacade (or equivalent aggregate) for a given DSN.
        The exact return type is intentionally erased here to avoid import cycles.
        """


class StorageProvider(NamedTuple):
    """Descriptor of a storage provider advertised under a DSN scheme."""

    name: str
    capabilities: StorageCapabilities
    factory: ProviderFactory


# ────────────────────────────────────── Registry ─────────────────────────────────────────


class PluginRegistry:
    """
    Central registry for adapters/transforms/externals/hook actions/handlers/storage providers.

    - Allows runtime registration via methods below.
    - Can load entry points ("flowkit.plugins", "flowkit.storage_providers").
    - Performs basic duplicate checks and name validation.
    """

    def __init__(self) -> None:
        self._sources: dict[str, type[SourceAdapter]] = {}
        self._sinks: dict[str, type[SinkAdapter]] = {}
        self._transforms: dict[str, type[TransformOp]] = {}
        self._externals: dict[str, Any] = {}
        self._hook_actions: dict[str, Any] = {}
        self._handlers: dict[str, type[BaseHandler]] = {}
        self._storage_providers: dict[str, StorageProvider] = {}

    # ─────────────── discovery ───────────────

    def load_entry_points(self) -> None:
        """
        Discover and register:
          - plugins declared under 'flowkit.plugins'
          - storage providers declared under 'flowkit.storage_providers'

        Safe to call multiple times; duplicates are ignored by raising RegistryError
        internally and continuing (best-effort discovery).
        """
        try:
            from importlib.metadata import entry_points  # py>=3.8
        except Exception as e:  # pragma: no cover - platform specific
            raise RegistryError(f"entry_points unavailable: {e}") from e

        eps = entry_points()
        # Plugins (Adapter bundles, hooks, externals, handlers)
        plugin_eps = eps.select(group="flowkit.plugins") if hasattr(eps, "select") else eps.get("flowkit.plugins", [])
        for ep in plugin_eps:
            try:
                plugin_cls = ep.load()
                plugin: Plugin = plugin_cls()  # type: ignore[call-arg]
                self.register_plugin(plugin)
            except Exception:
                # Swallow to keep discovery best-effort; userland logging can capture failures.
                continue

        # Storage providers
        sp_eps = (
            eps.select(group="flowkit.storage_providers")
            if hasattr(eps, "select")
            else eps.get("flowkit.storage_providers", [])
        )
        for ep in sp_eps:
            try:
                loaded = ep.load()
                # Accept a few common shapes:
                #  1) loaded is a StorageProvider-like object, and ep.name is the DSN scheme
                #  2) loaded() is a callable returning (scheme, StorageProvider)
                #  3) loaded is a mapping {scheme: StorageProvider}
                if callable(loaded):
                    scheme, provider = loaded()  # type: ignore[misc]
                    self.register_storage_provider(str(scheme), provider)
                elif isinstance(loaded, dict):
                    for scheme, provider in loaded.items():
                        self.register_storage_provider(str(scheme), provider)
                else:
                    scheme = getattr(loaded, "scheme", None) or str(ep.name)
                    self.register_storage_provider(scheme, loaded)
            except Exception:
                continue

    # ─────────────── registration helpers ───────────────

    def _check_name(self, name: str) -> None:
        if not name or name.strip() != name:
            raise RegistryError("empty/invalid name")
        if name.startswith("_"):
            raise RegistryError("names starting with '_' are reserved")

    # Plugins (bundle)

    def register_plugin(self, plugin: Plugin) -> None:
        """Register all components from a Plugin instance."""
        for name, cls in (plugin.sources() or {}).items():
            self.register_source(name, cls)
        for name, cls in (plugin.sinks() or {}).items():
            self.register_sink(name, cls)
        for name, cls in (plugin.transforms() or {}).items():
            self.register_transform(name, cls)
        for name, provider in (plugin.externals() or {}).items():
            self.register_external(name, provider)
        for name, action in (plugin.hook_actions() or {}).items():
            self.register_hook_action(name, action)
        for name, cls in (plugin.handlers() or {}).items():
            self.register_handler(name, cls)

    # Individual components

    def register_source(self, name: str, cls: type[SourceAdapter]) -> None:
        self._check_name(name)
        if name in self._sources:
            raise RegistryError(f"source already registered: {name}")
        self._sources[name] = cls

    def register_sink(self, name: str, cls: type[SinkAdapter]) -> None:
        self._check_name(name)
        if name in self._sinks:
            raise RegistryError(f"sink already registered: {name}")
        self._sinks[name] = cls

    def register_transform(self, name: str, cls: type[TransformOp]) -> None:
        self._check_name(name)
        if name in self._transforms:
            raise RegistryError(f"transform already registered: {name}")
        self._transforms[name] = cls

    def register_external(self, name: str, provider: Any) -> None:
        self._check_name(name)
        if name in self._externals:
            raise RegistryError(f"external already registered: {name}")
        self._externals[name] = provider

    def register_hook_action(self, name: str, action_cls: Any) -> None:
        self._check_name(name)
        if name in self._hook_actions:
            raise RegistryError(f"hook action already registered: {name}")
        self._hook_actions[name] = action_cls

    def register_handler(self, name: str, cls: type[BaseHandler]) -> None:
        self._check_name(name)
        if name in self._handlers:
            raise RegistryError(f"handler already registered: {name}")
        self._handlers[name] = cls

    # Storage providers

    def register_storage_provider(self, scheme: str, provider: StorageProvider) -> None:
        """
        Register a storage provider for a DSN scheme, e.g. "mongodb", "postgresql".
        """
        self._check_name(scheme)
        if scheme in self._storage_providers:
            raise RegistryError(f"storage provider already registered for scheme: {scheme}")
        # Basic sanity check
        if not getattr(provider, "factory", None):
            raise RegistryError("storage provider must define a 'factory' coroutine")
        self._storage_providers[scheme] = provider

    # ─────────────── lookups ───────────────

    def source(self, name: str) -> type[SourceAdapter]:
        try:
            return self._sources[name]
        except KeyError as e:
            raise RegistryError(f"unknown source: {name}") from e

    def sink(self, name: str) -> type[SinkAdapter]:
        try:
            return self._sinks[name]
        except KeyError as e:
            raise RegistryError(f"unknown sink: {name}") from e

    def transform(self, name: str) -> type[TransformOp]:
        try:
            return self._transforms[name]
        except KeyError as e:
            raise RegistryError(f"unknown transform: {name}") from e

    def external(self, name: str) -> Any:
        try:
            return self._externals[name]
        except KeyError as e:
            raise RegistryError(f"unknown external: {name}") from e

    def hook_action(self, name: str) -> Any:
        try:
            return self._hook_actions[name]
        except KeyError as e:
            raise RegistryError(f"unknown hook action: {name}") from e

    def handler(self, name: str) -> type[BaseHandler]:
        try:
            return self._handlers[name]
        except KeyError as e:
            raise RegistryError(f"unknown handler: {name}") from e

    def storage_provider(self, scheme: str) -> StorageProvider:
        try:
            return self._storage_providers[scheme]
        except KeyError as e:
            raise RegistryError(f"unknown storage provider scheme: {scheme}") from e

    # ─────────────── optional introspection ───────────────

    def list_sources(self) -> list[str]:
        return sorted(self._sources.keys())

    def list_sinks(self) -> list[str]:
        return sorted(self._sinks.keys())

    def list_transforms(self) -> list[str]:
        return sorted(self._transforms.keys())

    def list_externals(self) -> list[str]:
        return sorted(self._externals.keys())

    def list_hook_actions(self) -> list[str]:
        return sorted(self._hook_actions.keys())

    def list_handlers(self) -> list[str]:
        return sorted(self._handlers.keys())

    def list_storage_schemes(self) -> list[str]:
        return sorted(self._storage_providers.keys())
