#!/usr/bin/python

def fare_band(fare):
    if fare is None:    # handling NULL values
        return None
    try:    # handling non-numeric values
        f = float(fare)
    except:
        return None
    # mapping fare to: LOW (<=15), MID (>15 and <=30), and HIGH (>30)
    if f <= 15.0:
        return 'LOW'
    elif f <= 30.0:
        return 'MID'
    else:
        return 'HIGH'