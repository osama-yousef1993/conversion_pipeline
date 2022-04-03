FROM gcr.io/dataflow-templates-base/python39-template-launcher-base

ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="/template/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/firestore_sync.py"
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE="/template/setup.py"

WORKDIR /template
COPY requirements.txt ./requirements.txt

RUN apt-get install git

RUN sudo apt-get update \
    && sudo apt-get install -y libffi-dev git libsm6 libxext6 ffmpeg libfontconfig1 libxrender1 libgl1-mesa-glx \
    && rm -rf /var/lib/apt/lists/* && pip install -U --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -U -r $FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE \
    && pip download --no-cache-dir --dest /tmp/dataflow-requirements-cache -r $FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE

COPY . .
# Since we already downloaded all the dependencies, there's no need to rebuild everything.
ENV PIP_NO_DEPS=True
