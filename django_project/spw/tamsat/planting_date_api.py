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
    #column_name_pfc_mean = str('pfc_mean')
    #column_name_pfc_sd = str('pfc_std')

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
    output_domain='Region', pfc_thresh=70,
    pfc_prob_thresh=0.8, wrsi_thresh_factor=0.75,
    wrsi_prob_thresh=0.5, local_flag=0,
    csv_output=True
):
    """
    The routine_operations_v2() function runs the TAMSAT-ALERT
    planting date DST (Decision Support Tool)
    for user-defined locations and parameters,
    using soil moisture forecasts available at {tamsat_url}.

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
        This file must contain a column called 'Longitude' and
        a column called 'Latitude' (case sensitive).
        Other columns (e.g. farmer identifiers) may also be present.
    tamsat_url : str
        URL of the TAMSAT website where the soil moisture data is stored.
    working_dir : str
        Directory where the soil moisture data files will be stored.
    output_domain : str
        Domain string to be included in the output filename.
    pfc_thresh : float
        Threshold soil moisture considered suitable for planting,
        expressed as percent field capacity (range 0-100).
        For most regions, a sensible value is 70.
    pfc_prob_thresh : float
        Minimum probability that pfc_thresh will be exceeded
        in order for planting to be advised (range 0-1).
        For most regions, a sensible value is 0.8.
    wrsi_thresh_factor : float
        Fraction of the local maximum climatological WRSI to be attained
        for planting to be advised (range 0-1).
        For most regions, a sensible value is 0.75.
    wrsi_prob_thresh : float
        Minimum probability that the wrsi_thresh_factor will be exceeded
        in order for planting to be advised (range 0-1).
        For most regions, a sensible value is 0.5.
    local_flag : int, optional (default=0)
        If set to 1, local files are used; if set to 0,
        files are retrieved from the TAMSAT website.
        If local files are used, they must be present
        in the working directory.
    csv_output : bool, optional (default=True)
        If True, the output will be saved as a CSV file.

    Output
    ------
    A CSV file called <output_domain>_<yearin>_<monthin>_<dayin>.csv
    is produced.
    This file contains all columns from the input CSV file,
    plus additional columns:
        - pfc_probability
        - wrsi_probability
        - pfc_decision
        - wrsi_decision
        - overall_decision
        
    Returns
    -------
    Pandas dataframe that is output to the CSV file (see above)

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
        if os.path.exists(pfc_filepath) == False:
            input_file = pfc_filepath + str('.gz')
            output_file = pfc_filepath
            with gzip.open(input_file, 'rb') as f_in:
                with open(output_file, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
    if local_flag == 0:
        if os.path.exists(pfc_filepath) == False:
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
            except:
                print("cannot open pfc file")
                return None

            # Assign the filename variable to either the preliminary or
            # non-preliminary file and exit if neither exist
            if os.path.isfile(pfc_filepath) == False:
                print('soil moisture forecast file does not exist')
                return None

        # Make a local copy of the wrsi file if we don't have it
        if os.path.exists(wrsi_filepath) == False:
            response = urllib.request.urlopen(
                tamsat_url + str(WRSI_FILENAME)
            )

            wrsi_file = io.BytesIO(response.read())
            with open(wrsi_filepath, 'wb') as outfile:
                outfile.write(wrsi_file.read())

    planting_dst_output=xr.open_dataset(pfc_filepath)

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

    #No ECMWF forecasts
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

    arrayin_xr = planting_dst_output['pfc_risk_out']
    tmp = arrayin_xr.sel(
        longitude=np.array(obsdata['Longitude']),
        latitude=np.array(obsdata['Latitude']),
        method='nearest'
    )
    obslist = np.diag(tmp.squeeze())
    obsdata['pfc_noecmwf_probability'] = obslist

    arrayin_xr = planting_dst_output['wrsi_risk_out']
    tmp = arrayin_xr.sel(
        longitude=np.array(obsdata['Longitude']),
        latitude=np.array(obsdata['Latitude']),
        method='nearest'
    )
    obslist = np.diag(tmp.squeeze())
    obsdata['wrsi_noecmwf_probability'] = obslist

    arrayin_xr = planting_dst_output['pfc_decision']
    tmp = arrayin_xr.sel(
        longitude=np.array(obsdata['Longitude']),
        latitude=np.array(obsdata['Latitude']),
        method='nearest'
    )
    obslist = np.diag(tmp.squeeze())
    obsdata['pfc_noecmwf_decision'] = obslist

    arrayin_xr = planting_dst_output['wrsi_decision']
    tmp = arrayin_xr.sel(
        longitude=np.array(obsdata['Longitude']),
        latitude=np.array(obsdata['Latitude']),
        method='nearest'
    )
    obslist = np.diag(tmp.squeeze())
    obsdata['wrsi_noecmwf_decision'] = obslist

    arrayin_xr = planting_dst_output['overall_sm_decision']
    tmp = arrayin_xr.sel(
        longitude=np.array(obsdata['Longitude']),
        latitude=np.array(obsdata['Latitude']),
        method='nearest'
    )
    obslist = np.diag(tmp.squeeze())
    obsdata['overall_sm_noecmwf_decision'] = obslist
    
    arrayin_xr = planting_dst_output['pfc_ecmwf_risk_out']
    tmp = arrayin_xr.sel(
        longitude=np.array(obsdata['Longitude']),
        latitude=np.array(obsdata['Latitude']),
        method='nearest'
    )
    obslist = np.diag(tmp.squeeze())
    obsdata['pfc_ecmwf_probability'] = obslist

    arrayin_xr = planting_dst_output['pfc_ecmwf_decision']
    tmp = arrayin_xr.sel(
        longitude=np.array(obsdata['Longitude']),
        latitude=np.array(obsdata['Latitude']),
        method='nearest'
    )
    obslist = np.diag(tmp.squeeze())
    obsdata['pfc_ecmwf_decision'] = obslist

    arrayin_xr = planting_dst_output['overall_ecmwf_sm_decision']
    tmp = arrayin_xr.sel(
        longitude=np.array(obsdata['Longitude']),
        latitude=np.array(obsdata['Latitude']),
        method='nearest'
    )
    obslist = np.diag(tmp.squeeze())
    obsdata['overall_sm_ecmwf_decision'] = obslist
    
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

    obsdata = obsdata.round(2)
    if csv_output:
        csv_out = (
            str(output_domain) + str(datein.year) + str('_') +
            str(datein.month).zfill(2) + str('_') + str(datein.day).zfill(2) +
            str('_') + str('PFC') + str(pfc_thresh) + str('.csv')
        )
        obsdata.to_csv(os.path.join(working_dir, csv_out))
    obsdata = pds.DataFrame(obsdata)
    return obsdata
