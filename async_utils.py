import asyncio
import collections
import functools
import subprocess
import sys
from typing import Any, TextIO, cast


async def forward_and_return_data(
    output: TextIO | None, prefix: str, input: asyncio.StreamReader
) -> bytes:
    blocks: collections.deque[bytes] = collections.deque()
    space = " " * max(1, (30 - 2 - len(prefix)))
    while not input.at_eof():
        line = await input.readline()
        if len(line) == 0:
            break
        blocks.append(line)
        if output is not None:
            if not line.endswith(b"\n"):
                line += b"\n"
            output.write(f"[{prefix}]{space}{line.decode('utf-8')}")
    return b"".join(blocks)


async def kill_process_async(
    proc: asyncio.subprocess.Process, wait_before_kill_secs: float = 5.0
) -> None:
    if proc.returncode is not None:
        return

    # https://github.com/python/cpython/issues/88050
    # This is fixed in Python 3.11, but we're still using 3.10 in prod.
    transport: asyncio.SubprocessTransport | None = proc._transport  # type: ignore
    if transport is not None:
        transport.close()

    if wait_before_kill_secs > 0:
        try:
            proc.terminate()
        except ProcessLookupError:
            return
        await asyncio.wait_for(proc.wait(), wait_before_kill_secs)

        if cast(int | None, proc.returncode) is not None:
            return

    try:
        proc.kill()
    except ProcessLookupError:
        pass


@functools.wraps(asyncio.create_subprocess_exec)
async def run_subprocess_async(
    *cmd: str,
    input: bytes | None = None,
    wait_before_kill_secs: float = 5.0,
    capture_output: bool = True,
    **kwargs: Any,
) -> tuple[int, bytes, bytes]:
    """Runs a subprocess. Returns (exit_code, stdout_data, stderr_data)."""
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdin=asyncio.subprocess.PIPE,
        stdout=(asyncio.subprocess.PIPE if capture_output else None),
        stderr=(asyncio.subprocess.PIPE if capture_output else None),
        **kwargs,
    )
    try:
        stdout, stderr = await proc.communicate(input=input)
    finally:
        await kill_process_async(proc, wait_before_kill_secs=wait_before_kill_secs)

    assert proc.returncode is not None
    return (proc.returncode, stdout, stderr)


@functools.wraps(run_subprocess_async)
async def check_output_async(*cmd: str, **kwargs: Any) -> tuple[bytes, bytes]:
    retcode, stdout, stderr = await run_subprocess_async(*cmd, **kwargs)
    if retcode != 0:
        raise subprocess.CalledProcessError(retcode, cmd, stdout, stderr)
    return (stdout, stderr)


@functools.wraps(asyncio.create_subprocess_exec)
async def check_call_async(
    *args: str | bytes,
    prefix: str | None = None,
    input: bytes | None = None,
    wait_before_kill_secs: float = 5.0,
    suppress_stdout: bool = False,
    **kwargs,
) -> None:
    stderr_mode = asyncio.subprocess.PIPE if prefix else None
    stdout_mode = asyncio.subprocess.DEVNULL if suppress_stdout else stderr_mode
    input_mode = asyncio.subprocess.PIPE if input is not None else None
    proc = await asyncio.create_subprocess_exec(
        *args, stdin=input_mode, stdout=stdout_mode, stderr=stderr_mode, **kwargs
    )

    try:
        if input is not None:
            stdout = b""
            stderr = b""
            await proc.communicate(input=input)
            retcode = proc.returncode
        elif prefix:
            assert proc.stdout is not None
            assert proc.stderr is not None
            stdout, stderr, retcode = await asyncio.gather(
                forward_and_return_data(sys.stdout, f"{prefix} stdout", proc.stdout),
                forward_and_return_data(sys.stderr, f"{prefix} stderr", proc.stderr),
                proc.wait(),
            )
        else:
            assert proc.stdout is None
            assert proc.stderr is None
            stdout = b""
            stderr = b""
            retcode = await proc.wait()
    finally:
        await kill_process_async(proc, wait_before_kill_secs=wait_before_kill_secs)

    assert retcode is not None
    if retcode != 0:
        raise subprocess.CalledProcessError(retcode, args, stdout, stderr)
