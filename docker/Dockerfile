FROM python:3.11-slim-bullseye

LABEL org.opencontainers.image.authors="𝓑𝓵𝓪𝓬𝓴𝓞𝓻𝓭𝓮𝓻"
LABEL org.opencontainers.image.source="https://github.com/BlackOrder/mongo-cluster"

# add our user and group first to make sure their IDs get assigned consistently, regardless of whatever dependencies get added
RUN groupadd -r jasmin && useradd -r -g jasmin jasmin

# Install requirements
RUN apt-get update
RUN apt-get upgrade -y
RUN apt-get full-upgrade -y
RUN apt-get autoremove -y

RUN apt-get clean && rm -rf /var/lib/apt/lists/*

RUN /usr/local/bin/python -m pip install --upgrade pip

WORKDIR /build

COPY . .

RUN pip install .

# Clean up
RUN rm -rf /build

CMD ["jasminmongologd"]
