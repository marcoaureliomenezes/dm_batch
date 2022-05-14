#!/bin/bash

SIZE=$1

python /app/python/gen_fake_data.py --user root \
                      --password root \
                      --host mysql \
                      --port 3306 \
                      --db data_master \
                      --size $SIZE