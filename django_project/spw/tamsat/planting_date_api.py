# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Tamsat SPW Generator.
"""

import logging
import numpy as np
import shutil
import os
import datetime as dtmod
import xarray as xr
import uuid
import pandas as pds
import scipy
import gzip
import urllib
import io


logger = logging.getLogger(__name__)
WRSI_FILENAME = 'wrsi_daily_allafrica_0.25.nc'


def get_pfc_filename(datein):
    """Generate filename for PFC data based on date."""
    filename_pfc = (
        str('sm_forecast_jules_') + str(datein.year) + str('_') +
        str(datein.month).zfill(2) + str('_') + str(datein.day).zfill(2) +
        str('.nc')
    )
    return filename_pfc


def sm_decision(
    planting_dst_output, column_name_pfc_mean, column_name_pfc_sd,
    pfc_thresh, pfc_prob_thresh, wrsi_thresh_factor, wrsi_prob_thresh,
    working_dir
):
    """Calculate decision based on planting data output."""
    pfc_mean = planting_dst_output[column_name_pfc_mean].values
    pfc_sd = planting_dst_output[column_name_pfc_sd].values

    # Calculate risk of PFC/WRSI being unsuitable for planting based on
    # the user-defined threshold, and distribution mean and standard deviation

    pfc_risk_out = (scipy.stats.norm(pfc_mean, pfc_sd).cdf(pfc_thresh))
    pfc_decision = np.zeros(np.shape(pfc_risk_out))

    try:
        pfc_decision[((1 - pfc_risk_out) > pfc_prob_thresh)] = 1
    except Exception as e:
        logger.error(
            f"Error in calculating PFC decision: {e}, "
            f"pfc_risk_out: {pfc_risk_out}, "
            f"pfc_prob_thresh: {pfc_prob_thresh}",
            exc_info=True
        )
    wrsi_mean = planting_dst_output['wrsi_mean'].values
    wrsi_sd = planting_dst_output['wrsi_std'].values

    wrsi_all = xr.open_dataset(os.path.join(
        working_dir, WRSI_FILENAME
    ))
    wrsi_thresh = wrsi_thresh_factor * np.nanmax(
        wrsi_all['clim_mean_wrsi'], axis=2
    )

    wrsi_risk_out = (scipy.stats.norm(wrsi_mean, wrsi_sd).cdf(wrsi_thresh))

    wrsi_decision = np.zeros(np.shape(pfc_risk_out))
    wrsi_decision[((1 - wrsi_risk_out) > wrsi_prob_thresh)] = 1

    # Determine whether planting should be advised or not,
    # based on both criteria
    tmp = wrsi_decision + pfc_decision

    tmp[tmp < 2] = 0
    tmp[tmp == 2] = 1
    overall_decision = tmp
    return (
        pfc_risk_out, wrsi_risk_out, pfc_decision,
        wrsi_decision, overall_decision
    )


def routine_operations_v2(
    yearin, monthin, dayin, obsfile,
    tamsat_url, working_dir,
    output_domain='Region', pfc_thresh=70, pfc_prob_thresh=0.8,
    wrsi_thresh_factor=0.75, wrsi_prob_thresh=0.5, ecmwf_flag=1,
    local_flag=0, user_col=None, csv_output=True
):
    """
    Calculate the TAMSAT-ALERT planting date DST (Decision Support Tool).

    This functions uses user-defined locations and parameters,
    using all Africa soil moisture and WRSI forecasts
    available at: {tamsat_url}.

    This tool supports both local files and automatic download of required
    forecast files. Outputs include planting suitability decisions for
    different soil moisture thresholds and simplified outputs suitable for
    direct use by local partners (basic CSV file).

    Parameters
    ----------
    yearin : int
        Year for which the DST will be run.
    monthin : int
        Month for which the DST will be run.
    dayin : int
        Day for which the DST will be run.
    obsfile : str
        Full path to the CSV file containing the locations of the sites.
        This file must contain a column called 'Longitude' and a column
        called 'Latitude' (case sensitive).
        Other columns (e.g. farmer identifiers) may also be present.
    tamsat_url : str
        URL of the TAMSAT website where the soil moisture data is stored.
    working_dir : str
        Directory where the soil moisture data files will be stored.
    output_domain : str, optional (default='Region')
        Domain string to be included in the output filenames.
    pfc_thresh : float, optional (default=70)
        Threshold soil moisture considered suitable for planting, expressed as
        percent field capacity (range 0-100).
        For most regions, a sensible value is 70.
    pfc_prob_thresh : float, optional (default=0.8)
        Minimum probability that pfc_thresh will be exceeded in order for
        planting to be advised (range 0-1).
    wrsi_thresh_factor : float, optional (default=0.75)
        Fraction of the local maximum climatological WRSI to be attained for
        planting to be advised (range 0-1).
    wrsi_prob_thresh : float, optional (default=0.5)
        Minimum probability that the wrsi_thresh_factor will be exceeded in
        order for planting to be advised (range 0-1).
    ecmwf_flag : int, optional (default=1)
        If set to 1, ECMWF ensemble-based forecasts are used.
        If set to 0, non-ECMWF (climatology-based) forecasts are used.
    local_flag : int, optional (default=0)
        If set to 1, local files are used; if set to 0, files are retrieved
        from the TAMSAT website. If local files are used, they must be present
        in the working directory.
    user_col : str, optional (default=None)
        User-specified column from the input CSV file to be included in
        the "basic" CSV file. This allows users to include an additional
        identifying column (e.g. farmer reference, site name).
        The full CSV file will contain all columns
        from the original input file.
    csv_output : bool, optional (default=True)
        If True, the output will be saved as a CSV file.

    Output
    ------
    Two CSV files are produced (if `csv_output` is True):

    1. Full CSV file:
        <output_domain><yearin>_<monthin>_<dayin>_PFC<pfc_thresh>.csv
        - Contains *all columns from the input CSV file*.
        - Adds the following columns:
            - sm_25, sm_50, sm_70 : Planting suitability decisions at
            25%, 50%, and 70% PFC thresholds.
            - spw_20, spw_40, spw_60 : Short-period WRSI-based indicators.
            - pfc_probability and wrsi_probability (with or without
            ECMWF forecasts, depending on `ecmwf_flag`).
            - pfc_decision, wrsi_decision, overall_sm_decision
            (or their ECMWF equivalents).

    2. Basic CSV file:
        <output_domain><yearin>_<monthin>_<dayin>.csv
        - A simplified file intended for sharing with local partners.
        - Contains only the following columns:
            - Longitude
            - Latitude
            - sm_25
            - sm_50
            - sm_70
            - spw_20
            - spw_40
            - spw_60
            - user_col (specified by the `user_col` argument)

    Returns
    -------
    obsdata : pandas.DataFrame
        The full dataframe containing all columns, written to
        the "full CSV" file.
    obsdata_basic : pandas.DataFrame
        The simplified dataframe written to the "basic CSV" file.

    Example usage
    -------------
    yearin = 2025
    monthin = 6
    dayin = 3
    output_domain = 'kenya'
    obsfile = 'ke-test.csv'
    tamsat_url = ''
    working_dir = '/tmp/tamsat/'
    pfc_thresh = 70
    pfc_prob_thresh = 0.8
    wrsi_thresh_factor = 0.75
    wrsi_prob_thresh = 0.5
    local_flag = 0

    routine_operations_v2(yearin, monthin, dayin, obsfile, tamsat_url,
                          working_dir, output_domain,
                          pfc_thresh, pfc_prob_thresh, wrsi_thresh_factor,
                          wrsi_prob_thresh, local_flag=local_flag)

    Notes
    -----
    - The function will attempt to download required forecast files
    if `local_flag` is set to 0.
    - Forecast files are stored locally for reuse.
    - The basic CSV file can be used as a light-weight, shareable version of
    the output for extension services.
    - The function returns both DataFrames for further processing if needed.
    """
    # Set up the date that the planting date DST is to be run for
    datein = dtmod.datetime(int(yearin), int(monthin), int(dayin))

    # Read in the observed data
    obsdata = pds.read_csv(obsfile)

    # Assign file names for the preliminary and non-preliminary data
    filename_pfc = get_pfc_filename(datein)
    pfc_filepath = os.path.join(working_dir, filename_pfc)
    wrsi_filepath = os.path.join(working_dir, WRSI_FILENAME)

    # Assign URL names
    if local_flag == 1:
        if not os.path.exists(pfc_filepath):
            input_file = pfc_filepath + str('.gz')
            output_file = pfc_filepath
            with gzip.open(input_file, 'rb') as f_in:
                with open(output_file, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
    if local_flag == 0:
        if not os.path.exists(pfc_filepath):
            url = tamsat_url + str(filename_pfc) + str('.gz')
            # Stackoverflow guidance on gunzip in Python3
            # https://stackoverflow.com/questions/3548495/
            # download-extract-and-read-a-gzip-file-in-python
            # Download, decompress and extract data from
            # the gzipped soil moisture data
            try:
                tmp_filename = os.path.join(
                    working_dir,
                    str(uuid.uuid4())
                )
                response = urllib.request.urlopen(url)
                compressed_file = io.BytesIO(response.read())
                decompressed_file = gzip.GzipFile(fileobj=compressed_file)
                with open(tmp_filename, 'wb') as outfile:
                    outfile.write(decompressed_file.read())
                pfc = xr.open_dataset(tmp_filename)
                pfc.to_netcdf(pfc_filepath)
            except Exception as e:
                logger.error(
                    f"Error downloading or decompressing PFC data: {e}",
                    exc_info=True
                )
                return None, None

            # Assign the filename variable to either the preliminary or
            # non-preliminary file and exit if neither exist
            if not os.path.isfile(pfc_filepath):
                print('soil moisture forecast file does not exist')
                return None, None

        # Make a local copy of the wrsi file if we don't have it
        if not os.path.exists(wrsi_filepath):
            response = urllib.request.urlopen(
                tamsat_url + str(WRSI_FILENAME)
            )

            wrsi_file = io.BytesIO(response.read())
            with open(wrsi_filepath, 'wb') as outfile:
                outfile.write(wrsi_file.read())

    planting_dst_output = xr.open_dataset(pfc_filepath)

    # select the Correct cumulation and extract the data for this cumulation
    column_name_pfc_mean = str('pfc_mean')
    column_name_pfc_sd = str('pfc_std')
    (
        pfc_risk_out, wrsi_risk_out, pfc_decision,
        wrsi_decision, overall_sm_decision
    ) = sm_decision(
        planting_dst_output, column_name_pfc_mean, column_name_pfc_sd,
        pfc_thresh, pfc_prob_thresh, wrsi_thresh_factor, wrsi_prob_thresh,
        working_dir
    )

    column_name_pfc_mean = str('pfc_ecmwf_mean')
    column_name_pfc_sd = str('pfc_ecmwf_std')
    (
        pfc_ecmwf_risk_out, wrsi_risk_out, pfc_ecmwf_decision,
        wrsi_decision, overall_ecmwf_sm_decision
    ) = sm_decision(
        planting_dst_output, column_name_pfc_mean, column_name_pfc_sd,
        pfc_thresh, pfc_prob_thresh, wrsi_thresh_factor, wrsi_prob_thresh,
        working_dir
    )

    # Set up pre-determined thresholds
    column_name_pfc_mean = str('pfc_ecmwf_mean')
    column_name_pfc_sd = str('pfc_ecmwf_std')
    pfc_thresh_in = 70
    pfc_prob_thresh_in = 0.8
    wrsi_thresh_factor_in = 0.75
    wrsi_prob_thresh = 0.5

    (
        pfc_ecmwf_risk_out_70, wrsi_risk_out_70, pfc_ecmwf_decision_70,
        wrsi_decision_70, overall_ecmwf_sm_decision_70
    ) = sm_decision(
        planting_dst_output, column_name_pfc_mean, column_name_pfc_sd,
        pfc_thresh_in, pfc_prob_thresh_in, wrsi_thresh_factor_in,
        wrsi_prob_thresh, working_dir
    )

    column_name_pfc_mean = str('pfc_ecmwf_mean')
    column_name_pfc_sd = str('pfc_ecmwf_std')
    pfc_thresh_in = 50
    pfc_prob_thresh_in = 0.8
    wrsi_thresh_factor_in = 0.75
    wrsi_prob_thresh = 0.5

    (
        pfc_ecmwf_risk_out_50, wrsi_risk_out_50, pfc_ecmwf_decision_50,
        wrsi_decision_50, overall_ecmwf_sm_decision_50
    ) = sm_decision(
        planting_dst_output, column_name_pfc_mean, column_name_pfc_sd,
        pfc_thresh_in, pfc_prob_thresh_in, wrsi_thresh_factor_in,
        wrsi_prob_thresh, working_dir
    )

    column_name_pfc_mean = str('pfc_ecmwf_mean')
    column_name_pfc_sd = str('pfc_ecmwf_std')
    pfc_thresh_in = 25
    pfc_prob_thresh_in = 0.8
    wrsi_thresh_factor_in = 0.75
    wrsi_prob_thresh = 0.5

    (
        pfc_ecmwf_risk_out_25, wrsi_risk_out_25, pfc_ecmwf_decision_25,
        wrsi_decision_25, overall_ecmwf_sm_decision_25
    ) = sm_decision(
        planting_dst_output, column_name_pfc_mean, column_name_pfc_sd,
        pfc_thresh_in, pfc_prob_thresh_in, wrsi_thresh_factor_in,
        wrsi_prob_thresh, working_dir
    )

    # 70PFC
    planting_dst_output['pfc_ecmwf_risk_out_70'] = (
        ['longitude', 'latitude'], 1 - pfc_ecmwf_risk_out_70
    )
    planting_dst_output['wrsi_risk_out_70'] = (
        ['longitude', 'latitude'], 1 - wrsi_risk_out_70
    )
    planting_dst_output['pfc_ecmwf_decision_70'] = (
        ['longitude', 'latitude'], pfc_ecmwf_decision_70
    )
    planting_dst_output['wrsi_decision_70'] = (
        ['longitude', 'latitude'], wrsi_decision
    )
    planting_dst_output['overall_sm_decision_70'] = (
        ['longitude', 'latitude'], overall_ecmwf_sm_decision_70
    )

    # 50PFC
    planting_dst_output['pfc_ecmwf_risk_out_50'] = (
        ['longitude', 'latitude'], 1 - pfc_ecmwf_risk_out_50
    )
    planting_dst_output['wrsi_risk_out_50'] = (
        ['longitude', 'latitude'], 1 - wrsi_risk_out_50
    )
    planting_dst_output['pfc_ecmwf_decision_50'] = (
        ['longitude', 'latitude'], pfc_ecmwf_decision_50
    )
    planting_dst_output['wrsi_decision_50'] = (
        ['longitude', 'latitude'], wrsi_decision
    )
    planting_dst_output['overall_sm_decision_50'] = (
        ['longitude', 'latitude'], overall_ecmwf_sm_decision_50
    )

    # 25PFC
    planting_dst_output['pfc_ecmwf_risk_out_25'] = (
        ['longitude', 'latitude'], 1 - pfc_ecmwf_risk_out_25
    )
    planting_dst_output['wrsi_risk_out_25'] = (
        ['longitude', 'latitude'], 1 - wrsi_risk_out_25
    )
    planting_dst_output['pfc_ecmwf_decision_25'] = (
        ['longitude', 'latitude'], pfc_ecmwf_decision_25
    )
    planting_dst_output['wrsi_decision_25'] = (
        ['longitude', 'latitude'], wrsi_decision
    )
    planting_dst_output['overall_sm_decision_25'] = (
        ['longitude', 'latitude'], overall_ecmwf_sm_decision_25
    )

    # No ECMWF forecasts
    planting_dst_output['pfc_risk_out'] = (
        ['longitude', 'latitude'], 1 - pfc_risk_out
    )
    planting_dst_output['wrsi_risk_out'] = (
        ['longitude', 'latitude'], 1 - wrsi_risk_out
    )
    planting_dst_output['pfc_decision'] = (
        ['longitude', 'latitude'], pfc_decision
    )
    planting_dst_output['wrsi_decision'] = (
        ['longitude', 'latitude'], wrsi_decision
    )
    planting_dst_output['overall_sm_decision'] = (
        ['longitude', 'latitude'], overall_sm_decision
    )

    # Short term (PFC) with ECMWF forecasts
    planting_dst_output['pfc_ecmwf_risk_out'] = (
        ['longitude', 'latitude'], 1 - pfc_ecmwf_risk_out
    )
    planting_dst_output['pfc_ecmwf_decision'] = (
        ['longitude', 'latitude'], pfc_ecmwf_decision
    )
    planting_dst_output['overall_ecmwf_sm_decision'] = (
        ['longitude', 'latitude'], overall_ecmwf_sm_decision
    )

    arrayin_xr = planting_dst_output['overall_sm_decision_25']
    tmp = arrayin_xr.sel(
        longitude=np.array(obsdata['Longitude']),
        latitude=np.array(obsdata['Latitude']),
        method='nearest'
    )
    obslist = np.diag(tmp.squeeze())
    obsdata['sm_25'] = obslist

    arrayin_xr = planting_dst_output['overall_sm_decision_50']
    tmp = arrayin_xr.sel(
        longitude=np.array(obsdata['Longitude']),
        latitude=np.array(obsdata['Latitude']),
        method='nearest'
    )
    obslist = np.diag(tmp.squeeze())
    obsdata['sm_50'] = obslist

    arrayin_xr = planting_dst_output['overall_sm_decision_70']
    tmp = arrayin_xr.sel(
        longitude=np.array(obsdata['Longitude']),
        latitude=np.array(obsdata['Latitude']),
        method='nearest'
    )
    obslist = np.diag(tmp.squeeze())
    obsdata['sm_70'] = obslist

    arrayin_xr = planting_dst_output['spw_20']
    tmp = arrayin_xr.sel(
        longitude=np.array(obsdata['Longitude']),
        latitude=np.array(obsdata['Latitude']),
        method='nearest'
    )
    obslist = np.diag(tmp.squeeze())
    obsdata['spw_20'] = obslist

    arrayin_xr = planting_dst_output['spw_40']
    tmp = arrayin_xr.sel(
        longitude=np.array(obsdata['Longitude']),
        latitude=np.array(obsdata['Latitude']),
        method='nearest'
    )
    obslist = np.diag(tmp.squeeze())
    obsdata['spw_40'] = obslist

    arrayin_xr = planting_dst_output['spw_60']
    tmp = arrayin_xr.sel(
        longitude=np.array(obsdata['Longitude']),
        latitude=np.array(obsdata['Latitude']),
        method='nearest'
    )
    obslist = np.diag(tmp.squeeze())
    obsdata['spw_60'] = obslist

    if ecmwf_flag == 0:
        arrayin_xr = planting_dst_output['pfc_risk_out']
        tmp = arrayin_xr.sel(
            longitude=np.array(obsdata['Longitude']),
            latitude=np.array(obsdata['Latitude']),
            method='nearest'
        )
        obslist = np.diag(tmp.squeeze())
        obsdata['pfc_user_probability'] = obslist

        arrayin_xr = planting_dst_output['wrsi_risk_out']
        tmp = arrayin_xr.sel(
            longitude=np.array(obsdata['Longitude']),
            latitude=np.array(obsdata['Latitude']),
            method='nearest'
        )
        obslist = np.diag(tmp.squeeze())
        obsdata['wrsi_user_probability'] = obslist

        arrayin_xr = planting_dst_output['pfc_decision']
        tmp = arrayin_xr.sel(
            longitude=np.array(obsdata['Longitude']),
            latitude=np.array(obsdata['Latitude']),
            method='nearest'
        )
        obslist = np.diag(tmp.squeeze())
        obsdata['pfc_user_decision'] = obslist

        arrayin_xr = planting_dst_output['wrsi_decision']
        tmp = arrayin_xr.sel(
            longitude=np.array(obsdata['Longitude']),
            latitude=np.array(obsdata['Latitude']),
            method='nearest'
        )
        obslist = np.diag(tmp.squeeze())
        obsdata['wrsi_user_decision'] = obslist

        arrayin_xr = planting_dst_output['overall_sm_decision']
        tmp = arrayin_xr.sel(
            longitude=np.array(obsdata['Longitude']),
            latitude=np.array(obsdata['Latitude']),
            method='nearest'
        )
        obslist = np.diag(tmp.squeeze())
        obsdata['sm_user_decision'] = obslist

    if ecmwf_flag == 1:
        arrayin_xr = planting_dst_output['pfc_ecmwf_risk_out']
        tmp = arrayin_xr.sel(
            longitude=np.array(obsdata['Longitude']),
            latitude=np.array(obsdata['Latitude']),
            method='nearest'
        )
        obslist = np.diag(tmp.squeeze())
        obsdata['pfc_user_probability'] = obslist

        arrayin_xr = planting_dst_output['wrsi_risk_out']
        tmp = arrayin_xr.sel(
            longitude=np.array(obsdata['Longitude']),
            latitude=np.array(obsdata['Latitude']),
            method='nearest'
        )
        obslist = np.diag(tmp.squeeze())
        obsdata['wrsi_user_probability'] = obslist

        arrayin_xr = planting_dst_output['pfc_ecmwf_decision']
        tmp = arrayin_xr.sel(
            longitude=np.array(obsdata['Longitude']),
            latitude=np.array(obsdata['Latitude']),
            method='nearest'
        )
        obslist = np.diag(tmp.squeeze())
        obsdata['pfc_user_decision'] = obslist

        arrayin_xr = planting_dst_output['wrsi_decision']
        tmp = arrayin_xr.sel(
            longitude=np.array(obsdata['Longitude']),
            latitude=np.array(obsdata['Latitude']),
            method='nearest'
        )
        obslist = np.diag(tmp.squeeze())
        obsdata['wrsi_user_decision'] = obslist

        arrayin_xr = planting_dst_output['overall_ecmwf_sm_decision']
        tmp = arrayin_xr.sel(
            longitude=np.array(obsdata['Longitude']),
            latitude=np.array(obsdata['Latitude']),
            method='nearest'
        )
        obslist = np.diag(tmp.squeeze())
        obsdata['sm_user_decision'] = obslist

    # Select columns correctly using a list
    if user_col is not None:
        obsdata_basic = obsdata[
            [
                'Longitude',
                'Latitude',
                'sm_25',
                'sm_50',
                'sm_70',
                'spw_20',
                'spw_40',
                'spw_60',
                user_col
            ]
        ]
    else:
        obsdata_basic = obsdata[
            [
                'Longitude',
                'Latitude',
                'sm_25',
                'sm_50',
                'sm_70',
                'spw_20',
                'spw_40',
                'spw_60'
            ]
        ]

    # Round values
    obsdata_basic = obsdata_basic.round(2)
    obsdata = obsdata.round(2)
    if csv_output:
        csv_out = (
            str(output_domain) + str(datein.year) + str('_') +
            str(datein.month).zfill(2) + str('_') + str(datein.day).zfill(2) +
            str('_') + str('PFC') + str(pfc_thresh) + str('.csv')
        )
        csv_out_basic = (
            str(output_domain) + str(datein.year) + str('_') +
            str(datein.month).zfill(2) + str('_') + str(datein.day).zfill(2) +
            str('.csv')
        )
        # Write full obsdata
        obsdata.to_csv(os.path.join(working_dir, csv_out), index=False)
        # Write basic obsdata
        obsdata_basic.to_csv(
            os.path.join(working_dir, csv_out_basic), index=False
        )

    return obsdata, obsdata_basic
