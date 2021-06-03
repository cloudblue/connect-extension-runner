FROM python:3.8

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

WORKDIR /

RUN rm -rf /install_temp
