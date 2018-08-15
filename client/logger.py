import logging
import sys

# Prepare for logging
root = logging.getLogger()
root.setLevel(logging.INFO)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
fmt = '%(asctime)s %(levelname)s %(message)s'
formatter = logging.Formatter(fmt, '%y/%m/%d %H:%M:%S')
ch.setFormatter(formatter)
root.addHandler(ch)

# Create logging symbol to be exported
log = logging
