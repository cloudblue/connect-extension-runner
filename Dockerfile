FROM python:3.10-slim

ENV NODE_VERSION=16.17.1

RUN apt install -y curl
RUN curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.0/install.sh | bash
ENV NVM_DIR=/root/.nvm
RUN . "$NVM_DIR/nvm.sh" && nvm install ${NODE_VERSION}
RUN . "$NVM_DIR/nvm.sh" && nvm use v${NODE_VERSION}
RUN . "$NVM_DIR/nvm.sh" && nvm alias default v${NODE_VERSION}
ENV PATH="/root/.nvm/versions/node/v${NODE_VERSION}/bin/:${PATH}"
RUN node --version
RUN npm --version

ARG RUNNER_VERSION

RUN pip install -U pip && pip install poetry && mkdir -p /root/.config/pypoetry \
    && echo "[virtualenvs]" > /root/.config/pypoetry/config.toml \ 
    && echo "create = false" >> /root/.config/pypoetry/config.toml

COPY ./connect /install_temp/connect
COPY ./pyproject.toml /install_temp/.
COPY ./README.md /install_temp/.

WORKDIR /install_temp

RUN poetry version ${RUNNER_VERSION}

RUN poetry build

RUN pip install dist/*.whl

RUN rm -rf /install_temp

COPY ./entrypoint.sh /entrypoint.sh
RUN chmod 755 /entrypoint.sh

ENTRYPOINT [ "/entrypoint.sh" ]
