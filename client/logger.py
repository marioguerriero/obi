# Copyright 2018 
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

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
