# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

"""
Global plugin registry.

The registry is the extension discovery hub used by the compiler and runtime to
resolve adapter names, hook actions, and external providers.
"""

from typing import Any, Mapping

from .adapters import SourceAdapter, OutputAdapter, TransformOp, Plugin
from .errors import FlowkitError


class RegistryError(FlowkitError):
    """Raised when registration/lookup fails (duplicate/unknown/capability issues)."""

    ...


class PluginRegistry:
    """
    Central registry for adapters/transforms/externals/hook actions.

    - Supports runtime registration via methods below.
    - Optionally can load entry points ("flowkit.plugins") if desired by apps.
    - Performs basic duplicate checks and name validation.
    """

    def __init__(self) -> None:
        self._sources: dict[str, type[SourceAdapter]] = {}
        self._outputs: dict[str, type[OutputAdapter]] = {}
        self._transforms: dict[str, type[TransformOp]] = {}
        self._externals: dict[str, Any] = {}
        self._hook_actions: dict[str, Any] = {}

    # ---- discovery ---------------------------------------------------------

    def load_entry_points(self) -> None:
        """
        Discover and register plugins declared under the 'flowkit.plugins' entry point.

        Safe to call multiple times; duplicates are ignored with a warning.
        """
        try:
            from importlib.metadata import entry_points  # py>=3.8
        except Exception as e:  # pragma: no cover - platform specific
            raise RegistryError(f"entry_points unavailable: {e}") from e

        groups = entry_points()
        eps = groups.select(group="flowkit.plugins") if hasattr(groups, "select") else groups.get("flowkit.plugins", [])
        for ep in eps:
            try:
                plugin_cls = ep.load()
                plugin: Plugin = plugin_cls()  # type: ignore[call-arg]
                self.register_plugin(plugin)
            except Exception:
                # Best effort: do not explode registry on a single bad plugin
                continue

    # ---- registration ------------------------------------------------------

    def register_plugin(self, plugin: Plugin) -> None:
        """Register all components from a Plugin instance."""
        for name, cls in plugin.sources().items():
            self.register_source(name, cls)
        for name, cls in plugin.outputs().items():
            self.register_output(name, cls)
        for name, cls in plugin.transforms().items():
            self.register_transform(name, cls)
        for name, provider in plugin.externals().items():
            self.register_external(name, provider)
        for name, action in plugin.hook_actions().items():
            self.register_hook_action(name, action)

    def _check_name(self, name: str) -> None:
        if not name or name.strip() != name:
            raise RegistryError("empty/invalid name")
        if name.startswith("_"):
            raise RegistryError("names starting with '_' are reserved")

    def register_source(self, name: str, cls: type[SourceAdapter]) -> None:
        self._check_name(name)
        if name in self._sources:
            raise RegistryError(f"source already registered: {name}")
        self._sources[name] = cls

    def register_output(self, name: str, cls: type[OutputAdapter]) -> None:
        self._check_name(name)
        if name in self._outputs:
            raise RegistryError(f"output already registered: {name}")
        self._outputs[name] = cls

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

    # ---- lookups -----------------------------------------------------------

    def source(self, name: str) -> type[SourceAdapter]:
        """Lookup a source adapter class by name or raise RegistryError."""
        try:
            return self._sources[name]
        except KeyError as e:
            raise RegistryError(f"unknown source: {name}") from e

    def output(self, name: str) -> type[OutputAdapter]:
        try:
            return self._outputs[name]
        except KeyError as e:
            raise RegistryError(f"unknown output: {name}") from e

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
