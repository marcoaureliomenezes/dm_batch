PERIOD=$1

python stock_markets.py --user root \
                      --password root \
                      --host mysql \
                      --port 3306 \
                      --db data_master \
                      --period PERIOD