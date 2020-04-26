import rasterio as rio
import pandas as pd
import numpy as np
import copy
import multiprocessing

def reclassify_window(src_image, window, table, field):
    """
    accepts a full path to a src_image and a rasterio window
    (from rio.block_windows) to use for reclassification. Will
    map field values for each 'field' in a window using the matched
    entry from a 'table' and return the reclassified numpy array. This 
    is designed to work in parallel susing the multiprocessing library.
    """
    print(window)
    src_image = rio.open(src_image)
    w = src_image.read(1, window=window)
    result = copy.copy(w)
    for mu in np.sort(table["mukey"]):
        if np.sum(w == mu) == 0:
            print("Window didn't contain any valid mukey values",
                  "; remapping all values to 0")
            result.fill(0)
        else:
            print("Window contained value mukey values; remapping")
            result[(w == mu)] = table[table["mukey"] == mu][field]

    return result


def reclassify(src_image, table, field="di", dst_image=None):

    pool = multiprocessing.Pool(4)

    windows = [ w[1] for w in list(rio.open(src_image).block_windows(1)) ]

    results = list(pool.starmap(
        reclassify_window, [(src_image, w, table, field) for w in windows]
    ))

    if dst_image is not None:
        with rio.open(
            dst_image,
            "w",
            driver="GTiff",
            width=rio.open(src_image).width,
            height=rio.open(src_image).height,
            count=1,
            dtype=rio.open(src_image).dtypes[0],
        ) as dst:
            for i in range(results):
                dst.write(results[i], window=windows[i], indexes=1)
    else:
        return results

#
# MAIN
#

src_filename = "gssurgo_2020_mukey_30m.tif"

src = rio.open(src_filename)

src_params = {}
src_params['width'] = src.width
src_params['height'] = src.height
src_params['dtype'] = src.dtypes[0]

del src

di_pi_table = pd.read_csv("di_pi_table.csv")[["mukey", "Final_DI", "Final_PI"]]
di_pi_table = di_pi_table.rename(
    columns={"mukey": "mukey", "Final_DI": "di", "Final_PI": "pi"}
)

pool = multiprocessing.Pool(4)
windows = [w[1] for w in list(rio.open(src_filename).block_windows(1))]

results = list(pool.starmap(
    reclassify_window, [ (src_filename, w, di_pi_table, "di") for w in windows ]
))

with rio.open(
    "di_2020.tif",
    "w",
    driver="GTiff",
    width=src_params['width'],
    height=src_params['height'],
    count=1,
    dtype=src_params['dtype'],
) as dst:
    for i in range(results):
        dst.write(results[i], window=windows[i], indexes=1)

results = list(pool.starmap(
    reclassify_window, [ (src_filename, w, di_pi_table, "pi") for w in windows ]
))

with rio.open(
    "pi_2020.tif",
    "w",
    driver="GTiff",
    width=src_params['width'],
    height=src_params['height'],
    count=1,
    dtype=src_params['dtype'],
) as dst:
    for i in range(results):
        dst.write(results[i], window=windows[i], indexes=1)
