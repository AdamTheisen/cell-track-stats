import glob
import dask
import numpy as np
import act
import matplotlib.pyplot as plt
import pandas as pd
import os


def proc_data(filename):
    """
    This function is the processing routine that gathers data from a file and returns it
    so that this can be run using dask.

    Parameters
    ----------
    filename : str
        Filename to process

    Returns
    -------
    array : list
        Returns a list of values

    """

    # Read in the object using ACT and get some initial values
    obj = act.io.armfiles.read_netcdf(filename)
    scan_mode = obj.attrs['scan_mode']
    scan_name = obj.attrs['scan_name']
    template_name = obj.attrs['template_name']
    time = obj['time'].values

    # Perform some initial filtering on the data to try and smooth out ground clutter
    zh = obj['reflectivity']
    mask = obj['differential_reflectivity'].rolling(time=5).mean()
    zh = zh.where(mask <= 4.5, drop=False)
    mask = obj['copol_correlation_coeff'].rolling(time=5).mean()
    zh = zh.where(mask >= 0.98, drop=False)
    mask = obj['range']
    zh = zh.where(mask > 500, drop=False)
    obj['reflectivity'].values = zh.values

    # After filtering get some more data
    zh = obj['reflectivity']
    az = obj['azimuth'].values
    el = obj['elevation'].values
    rng = obj['range'].values

    # Calculating height for some statistics later on
    h = [(rng/1000.) * np.sin(np.deg2rad(e)) + ((rng/1000.) ** 2.) / (2. * 6370.) for e in el]

    # Get statistics and ranges 
    az_min = str(obj['azimuth'].min().values)
    az_max = str(obj['azimuth'].max().values)

    el_min = str(obj['elevation'].min().values)
    el_max = str(obj['elevation'].max().values)

    rng_min = str(obj['range'].min().values)
    rng_max = str(obj['range'].max().values)

    ngates_gt_0 = int(zh.where(zh > 0.).count())
    ngates_gt_10 = int(zh.where(zh > 10.).count())
    ngates_gt_30 = int(zh.where(zh > 30.).count())
    ngates_gt_50 = int(zh.where(zh > 50.).count())
    ngates_gt_10_5km = int(zh.where((zh >= 10.) & (np.array(h) > 5.)).count())
    ngates_gt_40_5km = int(zh.where((zh >= 40.) & (np.array(h) > 5.)).count())
    ngates = int(zh.count())
    try:
        # Find the location of the maximium reflectivity
        loc_max_zh = zh.argmax(dim=('time', 'range'))
        loc_max_zh_x = loc_max_zh['time'].values
        loc_max_zh_y = loc_max_zh['range'].values
        max_zh = str(obj['reflectivity'].max().values)
        if loc_max_zh_x.size > 1:
            loc_max_zh_x = loc_max_zh_x[0]
        if loc_max_zh_y.size > 1:
            loc_max_zh_y = loc_max_zh_y[0]
        # Plot data out if it's a cell-tracked scans
        if 'cell' in template_name:
            display = act.plotting.TimeSeriesDisplay(obj)
            title = str(time[0]) + ' ' + scan_name
            display.plot('reflectivity', set_title=title)
            display.axes[0].plot(time[loc_max_zh_x], rng[loc_max_zh_y], 'xr', markersize=10)

            date = filename.split('.')[-3]
            idir = '/home/theisen/www/cell_tracking/' + date
            if not os.path.exists(idir):
                os.makedirs(idir)
            writename = idir + '/' + filename.split('/')[-1] + '.png'
            plt.savefig(writename)
            plt.close('all')
        return [time[0], scan_mode, scan_name, template_name, az_min, az_max, el_min, el_max,
                rng_min, rng_max, az[loc_max_zh_x], rng[loc_max_zh_y], max_zh,
                ngates_gt_0, ngates_gt_10, ngates_gt_30, ngates_gt_50,
                ngates_gt_10_5km, ngates_gt_40_5km, ngates]
    except:
        loc_max_zh = np.nan
        loc_max_zh_x = np.nan
        loc_max_zh_y = np.nan
        max_zh = np.nan
        return [time[0], scan_mode, scan_name, template_name, az_min, az_max, el_min, el_max, 
                rng_min, rng_max, np.nan, np.nan, max_zh,
                ngates_gt_0, ngates_gt_10, ngates_gt_30, ngates_gt_50,
                ngates_gt_10_5km, ngates_gt_40_5km, ngates]



if __name__ == "__main__":
    # Grab the files based on a date
    dates = act.utils.dates_between('20220804', '20220824')

    for d in dates:
        d = d.strftime('%Y%m%d')
        print(d)
        files = glob.glob('/data/archive/hou/houcsapr2cfrS2.a1/*' + d + '*')
        files.sort()

        # Set up the dask processing
        task = []
        for f in files:
            task.append(dask.delayed(proc_data)(f))
            #result = proc_data(f)
            #print(result)
        results = dask.compute(*task)

        # Convert to a dataframe with column names and write to csv
        names = ['time', 'scan_mode', 'scan_name', 'template_name',
                 'azimuth_min', 'azimuth_max', 'elevation_min', 'elevation_max',
                 'range_min', 'range_max', 'cell_azimuth', 'cell_range', 'cell_zh',
                 'ngates_gt_0', 'ngates_gt_10', 'ngates_gt_30', 'ngates_gt_50',
                 'ngates_gt_10_5km', 'ngates_gt40_5km', 'ngates']
        df = pd.DataFrame(results, columns=names)
        output = './data/houcsapr.' + d + '.csv' 
        df.to_csv(output)
