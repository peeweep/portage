# Copyright 2026 Gentoo Authors
# Distributed under the terms of the GNU General Public License v2

import importlib
from asyncio import Event
from unittest.mock import patch

from portage.tests import TestCase
from portage.util.futures import asyncio

fetch_module = importlib.import_module("portage.package.ebuild.fetch")


class ConcurrentFetchTestCase(TestCase):
    def test_concurrency(self):
        async def main():
            active = 0
            peak = 0
            seen = []
            ready = Event()

            async def fetch(uris, settings, **kwargs):
                nonlocal active, peak
                active += 1
                peak = max(peak, active)
                seen.append(uris)
                if active == 2:
                    ready.set()
                await asyncio.wait_for(ready.wait(), 5)
                await asyncio.sleep(0)
                active -= 1
                return int("bad" not in uris)

            uris = {
                "a": ("https://one/a", "https://two/a"),
                "bad": ("https://one/bad",),
                "c": (),
            }
            with patch.object(fetch_module, "_async_fetch", fetch):
                result = await fetch_module.async_fetch(
                    uris, {"PORTAGE_FETCH_JOBS": "2"}, fetchonly=True
                )
            self.assertEqual(result, 0)
            self.assertEqual(peak, 2)
            self.assertCountEqual([next(iter(x)) for x in seen], uris)
            self.assertIn({"a": uris["a"]}, seen)

        asyncio.run(main())

    def test_cancel(self):
        async def main():
            active = set()
            started = Event()

            async def fetch(uris, settings, **kwargs):
                filename = next(iter(uris))
                active.add(filename)
                if len(active) == 2:
                    started.set()
                try:
                    await asyncio.Future()
                finally:
                    active.remove(filename)

            with patch.object(fetch_module, "_async_fetch", fetch):
                task = asyncio.ensure_future(
                    fetch_module.async_fetch(
                        {"a": (), "b": (), "c": ()}, {"PORTAGE_FETCH_JOBS": "2"}
                    )
                )
                await asyncio.wait_for(started.wait(), 5)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
            self.assertFalse(active)

        asyncio.run(main())

    def test_serial_modes(self):
        async def main():
            uris = {"a": (), "b": ()}
            for settings, listonly in (
                ({}, False),
                ({"PORTAGE_FETCH_JOBS": "0"}, False),
                ({"PORTAGE_FETCH_JOBS": "invalid"}, False),
                ({"PORTAGE_FETCH_JOBS": "2"}, True),
            ):

                async def fetch(files, settings, **kwargs):
                    self.assertIs(files, uris)
                    return 1

                with patch.object(fetch_module, "_async_fetch", fetch):
                    self.assertEqual(
                        await fetch_module.async_fetch(
                            uris, settings, listonly=listonly
                        ),
                        1,
                    )

        asyncio.run(main())

    def test_exception_cleanup(self):
        async def main():
            started = Event()
            cleaned = []

            async def fetch(uris, settings, **kwargs):
                if "bad" in uris:
                    await started.wait()
                    raise RuntimeError("fetch failed")
                try:
                    started.set()
                    await asyncio.Future()
                finally:
                    cleaned.append(True)

            with patch.object(fetch_module, "_async_fetch", fetch):
                with self.assertRaisesRegex(RuntimeError, "fetch failed"):
                    await fetch_module.async_fetch(
                        {"bad": (), "a": ()}, {"PORTAGE_FETCH_JOBS": "2"}
                    )
            self.assertEqual(cleaned, [True])

        asyncio.run(main())

    def test_legacy_uris(self):
        async def main():
            seen = {}

            async def fetch(uris, settings, **kwargs):
                seen.update(uris)
                return 1

            with patch.object(fetch_module, "_async_fetch", fetch):
                self.assertEqual(
                    await fetch_module.async_fetch(
                        iter(("https://one/a", "https://two/a", "b")),
                        {"PORTAGE_FETCH_JOBS": "2"},
                    ),
                    1,
                )
            self.assertEqual(seen, {"a": ["https://one/a", "https://two/a"], "b": []})

        asyncio.run(main())
