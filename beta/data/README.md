`coastline.json` contains a small Santa Barbara subset of Natural Earth's
public-domain 1:10m coastline, rounded to six decimal places. Coordinates are
[longitude, latitude]. Source:
https://github.com/nvkelso/natural-earth-vector/blob/master/geojson/ne_10m_coastline.geojson
https://www.naturalearthdata.com/about/terms-of-use/

Satellite navigation and tile locations follow CIRA SLIDER's GOES-18 full-disk
catalog and `bigPixels2LatLon` navigation, inspected September 24, 2026:
https://slider.cira.colostate.edu/js/define-products---rammb-slider.js
https://slider.cira.colostate.edu/js/rammb-slider.min.js
We use native zoom 5 (visible Band 2) and zoom 3 (nighttime microphysics), with
nearest-neighbor reprojection to a local north-up map. Nominal 0.5 / 2 km sample
spacing is at the satellite subpoint; Santa Barbara footprints are larger.

Day/night selection uses NOAA's approximate solar equations:
https://gml.noaa.gov/grad/solcalc/solareqns.PDF
Imagery near sunrise/sunset is explicitly labeled as twilight in the page.
