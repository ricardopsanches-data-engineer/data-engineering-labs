FROM kestra/kestra:latest

USER root

RUN apt-get update -y && apt-get install -y \
    python3-pip \
    curl \
    apt-transport-https \
    ca-certificates \
    gnupg

RUN pip install --no-cache-dir pandas pyarrow

RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" > /etc/apt/sources.list.d/google-cloud-sdk.list
RUN apt-get update -y && apt-get install -y google-cloud-cli