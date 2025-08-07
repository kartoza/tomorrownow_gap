# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Common functions for Google Nowcast Ingestor.
"""


def get_forecast_target_time_from_filename(filename):
    """
    Extracts the forecast target time from the filename.

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
