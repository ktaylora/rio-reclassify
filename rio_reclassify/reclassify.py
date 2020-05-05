
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

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

SETTINGS = {
    'THREAD_COUNT': round(multiprocessing.cpu_count() * 0.75)
}

def arbitrary_window_size(src_image, col_off=0, row_off=0, k=15):
    """
    pick an arbitrary window size that will chunk
    a raster into ~k**2 equal blocks
    """
    src_image = rio.open(src_image)
    return Window(col_off, row_off, round(src_image.width / k),
        round(src_image.height / k))


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
    for value in np.sort(table[field]):
        if np.sum(w == value) == 0:
            LOGGER.debug("Window didn't contain any valid values"+
                  "; remapping all values to 0")
            result.fill(0)
        else:
            LOGGER.debug("Window contained valid; remapping")
            result[(w == value)] = table[table[field] == value][field]

    return result


def reclassify(src_image, table, field, band=1, dst_image=None):

    pool = multiprocessing.Pool(SETTINGS['THREAD_COUNT'])

    windows = [ w[1] for w in list(rio.open(src_image).block_windows(
        band, window=arbitrary_window_size(src_image))) ]

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
            dtype=rio.open(src_image).dtypes[band-1],
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