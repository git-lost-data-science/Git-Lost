import asyncio
import sys

from colorama import Fore
from getkey import getkey

DEFAULT_BLAZEGRAPH_ARGS = ("java", "-server", "-Xms4g", "-Xmx8g", "-jar", "blazegraph.jar")

started_event = asyncio.Event()

def stop_process_on_keypress(process: asyncio.subprocess.Process):
    print(Fore.MAGENTA + "Press any key to stop this process..." + Fore.RESET)
    started_event.set()
    _ = getkey()
    process.kill()

async def create_blazegraph_process(*args: str) -> asyncio.subprocess.Process:
    return await asyncio.create_subprocess_exec(
        *args, 
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

async def print_process_output(process: asyncio.subprocess.Process):
    assert process.stdout is not None
    await started_event.wait()
    try:
        async for line in process.stdout:
            print(Fore.BLUE + line.decode() + Fore.RESET, end="")
            sys.stdout.flush()
    except:
        pass

async def print_process_errors(process: asyncio.subprocess.Process):
    assert process.stderr is not None
    await started_event.wait()
    try:
        async for line in process.stderr:
            print(Fore.RED + line.decode() + Fore.RESET, end="")
            sys.stdout.flush()
    except:
        pass

async def main(args: list[str] = DEFAULT_BLAZEGRAPH_ARGS):
    process = await create_blazegraph_process(*args)
    stop_thread = asyncio.to_thread(stop_process_on_keypress, process)
    process_stdout = asyncio.create_task(print_process_output(process))
    process_stderr = asyncio.create_task(print_process_errors(process))
    await asyncio.gather(stop_thread, process_stdout, process_stderr)

if __name__ == "__main__":
    asyncio.run(main())
