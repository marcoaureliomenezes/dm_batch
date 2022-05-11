#!/bin/bash

SIZE=$1

python gen_clients.py --user root \
                      --password root \
                      --host mysql \
                      --port 3306 \
                      --db data_master \
                      --size $SIZE