FROM hdfs-base
WORKDIR /root
RUN apt-get -q update \
    && apt-get -q install -y --no-install-recommends curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
RUN curl https://sh.rustup.rs -sSf | sh -s -- --default-toolchain stable -y
