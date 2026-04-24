"""OpenCV-based chart-to-series extraction for sources that publish data
only as images (e.g. YPFB indicadores, many BCB slides).

Two modes:
  * extract_bar_values(image, y_axis_calibration)
      Detect vertical bars by color thresholding and measure bar height
      in pixel space; convert to data space via two anchor (pixel, value)
      pairs on the y-axis.
  * extract_line_values(image, line_bgr, x_ticks, y_axis_calibration)
      For a given line color and set of x-axis tick pixel columns, find
      the median y-pixel of that color in each column window and
      convert to data space.

Both modes require manual axis calibration. Pass anchors as lists of
(pixel_y, value) tuples. No OCR on axis labels; that would require
Tesseract. Calibration is one-time per chart template.
"""
from __future__ import annotations

from dataclasses import dataclass

import cv2
import numpy as np


@dataclass
class AxisCalibration:
    # Two anchor pairs for linear interpolation between pixel and data space.
    pixel_low: int
    value_low: float
    pixel_high: int
    value_high: float

    def to_value(self, pixel: float) -> float:
        span_px = self.pixel_high - self.pixel_low
        span_val = self.value_high - self.value_low
        return self.value_low + (pixel - self.pixel_low) * span_val / span_px


def _color_mask(img_bgr: np.ndarray, target_bgr: tuple[int, int, int],
                tolerance: int = 40) -> np.ndarray:
    """Boolean mask where every channel is within `tolerance` of the target."""
    b, g, r = target_bgr
    lower = np.array([max(0, b - tolerance), max(0, g - tolerance), max(0, r - tolerance)])
    upper = np.array([min(255, b + tolerance), min(255, g + tolerance), min(255, r + tolerance)])
    return cv2.inRange(img_bgr, lower, upper)


def extract_line_values(image_path: str,
                        line_bgr: tuple[int, int, int],
                        x_ticks: list[int],
                        y_axis: AxisCalibration,
                        x_window: int = 12,
                        tolerance: int = 40) -> list[float]:
    """For a series drawn in color `line_bgr`, read its y-value at each
    x-tick column (averaged over ±x_window pixels).
    Returns a list of data-space values, same length as x_ticks."""
    img = cv2.imread(image_path)
    if img is None:
        raise RuntimeError(f"cannot read {image_path}")
    mask = _color_mask(img, line_bgr, tolerance=tolerance)
    out: list[float] = []
    for xc in x_ticks:
        x0 = max(0, xc - x_window); x1 = min(mask.shape[1], xc + x_window)
        col_block = mask[:, x0:x1]
        ys, _ = np.nonzero(col_block)
        if len(ys) == 0:
            out.append(float("nan"))
            continue
        # Median row index — robust to anti-aliasing edges
        y_med = float(np.median(ys))
        out.append(y_axis.to_value(y_med))
    return out


def extract_bar_heights(image_path: str,
                        bar_bgr: tuple[int, int, int],
                        x_bar_centers: list[int],
                        y_axis: AxisCalibration,
                        x_window: int = 10,
                        tolerance: int = 40) -> list[float]:
    """For vertical bars drawn in color `bar_bgr`, read the top-edge y-coord
    at each given bar center x-pixel, convert to data value."""
    img = cv2.imread(image_path)
    mask = _color_mask(img, bar_bgr, tolerance=tolerance)
    out: list[float] = []
    for xc in x_bar_centers:
        x0 = max(0, xc - x_window); x1 = min(mask.shape[1], xc + x_window)
        col_block = mask[:, x0:x1]
        ys = np.nonzero(col_block.any(axis=1))[0]
        if len(ys) == 0:
            out.append(float("nan"))
            continue
        y_top = float(ys.min())
        out.append(y_axis.to_value(y_top))
    return out


if __name__ == "__main__":
    pass
