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

def reclassify_worker:
    pass
