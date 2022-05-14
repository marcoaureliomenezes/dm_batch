PERIOD=$1

python /app/python/get_daily_markets.py --user root \
                      --password root \
                      --host mysql \
                      --port 3306 \
                      --db data_master \
                      --period PERIOD
