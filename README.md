# cell-track-stats
The Atmospheric Radiation Measurement user facility (ARM) deployed one of the ARM Mobile Facilities (AMF1) to the Houston, TX region in support of the TRacking Aerosol Convection interactions ExpeRiment (TRACER).  As part of this campaign, the development and implementation of automated cell-tracking techniques were implemented with ARM's C-Band system (CSAPR2).  It will be important for ARM and the science communnity to understand the location of the cell(s) tracked.  This is a preliminary program to gather cell-location and a variety of other statistics noted below until the more formal metadata is made available by the cell-tracking team.

This script is very simple in that after thresholding the data by the following, it looks for the location of the max reflectivity.  Images are generated for the cell-tracked files and located [here](https://dev.arm.gov/~theisen/cell_tracking/) for verification where the red x marks the location of the cell.

- Smoothed Zdr (time=5) < 4.5 dB
- Smoothed RhoHV (time=4) > 0.98
- Range > 500 m

Currently the files in the data directory adhere to the following format

- index: Index based on the pandas dataframe that wrote it out
- time: Date/time of the scan
- scan_mode: Scan mode reported by the radar
- scan_name: Scan name reported by the radar which looks to be the same as scan_mode
- template_name: Name of the scan defined on the instrument computer
- azimuth_min: Minimum of azimuth for the entire scan
- azimuth_max: Maximum of the azimuth for the entire scan
- elevation_min: Minimum of elevation for the entire scan
- elevation_max: Maximum of elevation for the entire scan
- range_min: Minimum of range for the entire scan
- range_max: Maximum of range for the entire scan
- cell_azimuth: Azimuth location of the max Zh that is defined as the cell
- cell_range: Range of the max Zh that is defined as the cell
- cell_zh: Max of the reflectivity of the cell
- ngates_gt_0: Number of gates in the file greater than 0 dBZ
- ngates_gt_10: Number of gates in the file greater than 10 dBZ
- ngates_gt_30: Number of gates in the file greater than 30 dBZ
- ngates_gt_50: Number of gates in the file greater than 50 dBZ
- ngates_gt_10_5km: Number of gates in the file greater than 10 dBZ and greater than 5 km from the ground
- ngates_gt_40_5km: Number of gates in the file greater than 40 dBZ and greater than 5 km from the ground
- ngates: Number of gates in the file
