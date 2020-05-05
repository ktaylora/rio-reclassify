
from __future__ import division, absolute_import
import logging
import os
import copy
import numpy as np
import rasterio as rio
from rasterio.windows import Window
from rasterio.transform import guard_transform
from .utils import cs_forward, cs_backward
import multiprocessing

def arbitrary_window_size(src_image, k=15):
    """
    build an arbitrary window size that will chunk
    a raster into ~k equal blocks
    """
    def even(step=1):
        return (2*1250*1000**(1/step))
    def odd(step=1):
        return (3*2187*999**(1/step))

    src_image = rio.open(src_image)

    if src_image.width % 2 == 0:
        i = 1
        while src_image.width / even(i) <= k:
            i+=1
        width = round(even(i))
    else:
        i = 1
        while src_image.width / odd(i) <= k:
            i+=2
        width = round(odd(i))
    if src_image.height % 2 == 0:
        i = 1
        while src_image.height / even(i) <= k:
            i+=1
        height = round(even(i))
    else:
        i = 1
        while src_image.height / odd(i) <= k:
            i+=2
        height = round(odd(i))

    return Window(0, 0, width, height)

def reclassify_window(src_image, window, table, field, band=1):
    """
    accepts a full path to a src_image and a rasterio window
    (from rio.block_windows) to use for reclassification. Will
    map field values for each 'field' in a window using the matched
    entry from a 'table' and return the reclassified numpy array. This 
    is designed to work in parallel susing the multiprocessing library.
    """
    src_image = rio.open(src_image)
    w = src_image.read(band, window=window)
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


def reclassify(src_image, table, field="di", band=1, dst_image=None):

    pool = multiprocessing.Pool(4)

    windows = [ w[1] for w in list(rio.open(src_image).block_windows(band, window=arbitrary_window_size(src_image))) ]

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

def reclassify_worker(src_path, ref_path, dst_path, match_proportion,
                      creation_options, bands, color_space, plot):
                      """
                      """
                      pass