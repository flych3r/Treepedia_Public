import asyncio

MAX_CONCURRENT = 500


async def gather_with_concurrency(*tasks):
    """
    Limits number of concurrent calls

    Parameters
    ----------
    tasks: any
        function to execute concurrently
    """
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    async def sem_task(task):
        async with semaphore:
            return await task

    return await asyncio.gather(*(sem_task(task) for task in tasks))
