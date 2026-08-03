from __future__ import annotations

import argparse
import asyncio
import time

import synor as syn


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be a positive integer")
    return parsed


async def _identity(value: int) -> int:
    return value


async def _run(item_count: int, max_in_flight: int) -> None:
    count = 0
    checksum = 0
    started_at = time.perf_counter()

    async for value in syn.map_stream(
        _identity,
        range(item_count),
        max_in_flight,
    ):
        count += 1
        checksum += value

    elapsed = time.perf_counter() - started_at
    expected_checksum = item_count * (item_count - 1) // 2
    if count != item_count or checksum != expected_checksum:
        raise RuntimeError("map_stream benchmark produced an unexpected result")

    print(f"items={count}")
    print(f"max_in_flight={max_in_flight}")
    print(f"checksum={checksum}")
    print(f"elapsed_seconds={elapsed:.6f}")
    print(f"items_per_second={count / elapsed:.0f}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark bounded completion-order map streaming."
    )
    parser.add_argument("--items", type=_positive_int, default=1_000_000)
    parser.add_argument("--max-in-flight", type=_positive_int, default=64)
    args = parser.parse_args()
    asyncio.run(_run(args.items, args.max_in_flight))


if __name__ == "__main__":
    main()
