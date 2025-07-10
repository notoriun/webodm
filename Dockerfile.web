ARG VER_PGI_INFRA=1.0.0

FROM notoriun/pgi_infra:${VER_PGI_INFRA}

ARG TEST_BUILD

COPY ./requirements.txt /webodm/requirements.txt
COPY ./package.json ./package-lock.json /webodm/

RUN pip install -r requirements.txt "boto3==1.14.14" ffmpeg-python && \
    npm install --quiet -g webpack@5.89.0 webpack-cli@5.1.4 && \
    npm ci --quiet

COPY . /webodm/

RUN ln -s /webodm/nginx/crontab /var/spool/cron/crontabs/root && \
    chmod 0644 /webodm/nginx/crontab && \
    chmod +x /webodm/nginx/letsencrypt-autogen.sh && \
    /webodm/nodeodm/setup.sh && \
    /webodm/nodeodm/cleanup.sh && \
    cd /webodm && \
    webpack --mode production && \
    echo "UTC" > /etc/timezone && \
    python manage.py collectstatic --noinput && \
    python manage.py rebuildplugins && \
    python manage.py translate build --safe && \
    # Cleanup
    apt remove -y g++ python3-dev libpq-dev && \
    apt autoremove -y && \
    apt clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* && \
    rm /webodm/webodm/secret_key.py

VOLUME /webodm/app/media
