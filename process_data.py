import glob
import dask
import numpy as np
import act
import matplotlib.pyplot as plt
import pandas as pd


def proc_data(filename):
    obj = act.io.armfiles.read_netcdf(filename)
    scan_mode = obj.attrs['scan_mode']
    scan_name = obj.attrs['scan_name']
    template_name = obj.attrs['template_name']

    time = obj['time'].values
    zh = obj['reflectivity']
    mask = obj['differential_reflectivity'].rolling(time=5).mean()
    zh = zh.where(mask <= 4.5, drop=False)
    mask = obj['copol_correlation_coeff'].rolling(time=5).mean()
    zh = zh.where(mask >= 0.98, drop=False)
    mask = obj['range']
    zh = zh.where(mask > 500, drop=False)
    obj['reflectivity'].values = zh.values

    #idx = obj['sweep_end_ray_index'].values
    #if 'ppi' in scan_name:
    #    obj = obj.where(obj['time'] < obj['time'].values[int(idx[0])], drop=True)

    zh = obj['reflectivity']

    az = obj['azimuth'].values
    rng = obj['range'].values
    az_min = str(obj['azimuth'].min().values)
    az_max = str(obj['azimuth'].max().values)

    el_min = str(obj['elevation'].min().values)
    el_max = str(obj['elevation'].max().values)

    try:
        loc_max_zh = zh.argmax(dim=('time', 'range'))
        loc_max_zh_x = loc_max_zh['time'].values
        loc_max_zh_y = loc_max_zh['range'].values
        max_zh = str(obj['reflectivity'].max().values)
        if loc_max_zh_x.size > 1:
            loc_max_zh_x = loc_max_zh_x[0]
        if loc_max_zh_y.size > 1:
            loc_max_zh_y = loc_max_zh_y[0]
        if 'cell' in template_name:
            display = act.plotting.TimeSeriesDisplay(obj)
            title = str(time[0]) + ' ' + scan_name
            display.plot('reflectivity', set_title=title)
            display.axes[0].plot(time[loc_max_zh_x], rng[loc_max_zh_y], 'xr', markersize=10)
            writename = '/home/theisen/www/cell_tracking/' + filename.split('/')[-1] + '.png'
            plt.savefig(writename)
            plt.close('all')
        return [time[0], scan_mode, scan_name, template_name, az_min, az_max, el_min, el_max, az[loc_max_zh_x], rng[loc_max_zh_y], max_zh]
    except:
        loc_max_zh = np.nan
        loc_max_zh_x = np.nan
        loc_max_zh_y = np.nan
        max_zh = np.nan
        return [time[0], scan_mode, scan_name, template_name, az_min, az_max, el_min, el_max, np.nan, np.nan, max_zh]



if __name__ == "__main__":
    date = '20220604'
    files = glob.glob('/data/archive/hou/houcsapr2cfrS2.a1/*' + date + '*')
    files.sort()
    task = []
    for f in files:
        task.append(dask.delayed(proc_data)(f))
        #result = proc_data(f)
        #print(result)
    results = dask.compute(*task)

    names = ['time', 'scan_mode', 'scan_name', 'template_name', 'azimuth_min', 'azimith_max', 'elevation_min', 'elevation_max',
             'cell_azimuth', 'cell_range', 'cell_zh']
    df = pd.DataFrame(results, columns=names)
    output = '/home/theisen/www/cell_tracking/csv/houcsapr.' + date + '.csv' 
    df.to_csv(output)
