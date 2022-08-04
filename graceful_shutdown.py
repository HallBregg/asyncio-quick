import asyncio
import logging
import signal
import sys
from typing import NoReturn, Iterable

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger(__name__)


async def worker(no: int) -> NoReturn:
    """
    Simple async infinite worker.
    :param no: ID number
    """
    logger.debug(f'Worker: {no} started.')
    while True:
        try:
            logger.debug(f'Worker: {no} alive.')
            # Abstraction on working.
            await asyncio.sleep(1)
        except asyncio.CancelledError:
            logger.debug(f'Worker: {no} cleaning.')
            # Abstraction on cleaning worker.
            await asyncio.sleep(2)
            logger.debug(f'Worker: {no} cancelled.')
            break
        except Exception as e:
            logger.exception(f'Exception occurred: {str(e)}')


async def graceful_shutdown(sig: int, exclude: Iterable[str] = None) -> NoReturn:
    """
    Start shutdown process. Get all running tasks except the current one and excluded ones.
    Cancel tasks and wait for finish.

    :param sig: Signal that is handled.
    :param exclude: List of method names to be excluded from cancellation.
    """
    exclude: Iterable = exclude or []

    logger.info(f'Received: {sig} signal. Starting shutdown.')

    # TODO Building a list of tasks to cancel does not seem to be a good solution.
    #  Especially with "exclude" method.
    currently_running_tasks: list = [
        task for task in asyncio.all_tasks()
        if task is not asyncio.current_task() and task._coro.__name__ not in exclude
    ]
    logger.info(f'Currently running tasks: {len(currently_running_tasks)}.')
    for task in currently_running_tasks:
        task.cancel()
    logger.info('Tasks cancelled.')
    logger.info('Waiting for tasks to finish.')

    await asyncio.gather(*currently_running_tasks)
    logger.info('Shutdown completed.')


async def main() -> NoReturn:
    """
    Entrypoint. Initialize signals and handlers. Create and start workers (tasks).
    """
    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()

    shutdown_signals = (signal.SIGHUP, signal.SIGINT, signal.SIGTERM)
    for shutdown_signal in shutdown_signals:
        logger.info(f'Initializing handler for signal: {shutdown_signal}')
        loop.add_signal_handler(
            sig=shutdown_signal,
            callback=lambda *_: asyncio.create_task(
                coro=graceful_shutdown(sig=shutdown_signal, exclude=['main'])
            )
        )

    await asyncio.gather(
        asyncio.create_task(worker(1)),
        asyncio.create_task(worker(2)),
        asyncio.create_task(worker(3)),
    )
    logger.info('Main task cancelled.')


if __name__ == '__main__':
    asyncio.run(main())
