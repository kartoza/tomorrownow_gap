# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Common functions for Google Nowcast Ingestor.
"""


def get_forecast_target_time_from_filename(filename):
    """Extract the forecast target time from the filename.

    Parameters:
    -----------
    filename : str
        The filename containing the forecast target time.

    Returns:
    --------
    int
        The forecast target time as a Unix timestamp.
    """
    # Example filename: 'nowcast_1754434800_0.tif'
    if not filename.startswith('nowcast_'):
        raise ValueError(
            f"Filename '{filename}' does not start with 'nowcast_'."
        )

    parts = filename.replace('.tif', '').split('_')
    if len(parts) < 3:
        raise ValueError(
            f"Filename '{filename}' does not contain a valid "
            "forecast target time."
        )

    try:
        return int(parts[1]) + int(parts[2])
    except ValueError:
        raise ValueError(
            f"Invalid forecast target time in filename '{filename}'."
        )


def get_forecast_time_from_filename(filename):
    """Extract the forecast time from the filename for graphcast.

    Parameters:
    -----------
    filename : str
        The filename containing the forecast time.

    Returns:
    --------
    int
        The forecast time as a Unix timestamp.
    """
    # Example filename: 'graphcast_1754434800_6.tif'
    # 1754434800 is the start time in epoch
    # 6 is the forecast hour
    if not filename.startswith('graphcast_'):
        raise ValueError(
            f"Filename '{filename}' does not start with 'graphcast_'."
        )

    parts = filename.replace('.tif', '').split('_')
    if len(parts) < 3:
        raise ValueError(
            f"Filename '{filename}' does not contain a valid "
            "forecast target time."
        )

    try:
        return int(parts[1]) + (int(parts[2]) * 3600)
    except ValueError:
        raise ValueError(
            f"Invalid forecast target time in filename '{filename}'."
        )
