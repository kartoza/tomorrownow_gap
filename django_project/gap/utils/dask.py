# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Dask Utils.
"""

import dask
from dask.delayed import Delayed
from concurrent.futures import ThreadPoolExecutor

from gap.models import Preferences


def get_num_of_threads(is_api=False):
    """Get number of threads for dask computation.

    :param is_api: whether for API usage, defaults to False
    :type is_api: bool, optional
    """
    preferences = Preferences.load()
    return (
        preferences.dask_threads_num_api if is_api else
        preferences.dask_threads_num
    )



def execute_dask_compute(x: Delayed, is_api=False, dask_num_threads=None):
    """Execute dask computation based on number of threads config.

    :param x: Dask delayed object
    :type x: Delayed
    :param is_api: Whether the computation is in GAP API, default to False
    :type is_api: bool
    :param dask_num_threads: Threads number to use, if None will use config
    :type dask_num_threads: int
    """
    if dask_num_threads is not None:
        # use the number of threads from the parameter
        num_of_threads = dask_num_threads
    else:
        num_of_threads = get_num_of_threads(is_api)

    if num_of_threads <= 0:
        # use everything
        x.compute()
    else:
        with dask.config.set({
            "distributed.scheduler.worker-saturation": 1,
            "pool": ThreadPoolExecutor(num_of_threads),
        }):
            x.compute()
