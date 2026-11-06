# The orders_live job's image: the Flex Template launcher and the workers run this same image (the
# job sets it as its SDK container), so nothing is installed when a job starts.
FROM gcr.io/dataflow-templates-base/python311-template-launcher-base:latest AS launcher
FROM apache/beam_python3.11_sdk:2.60.0
COPY --from=launcher /opt/google/dataflow/python_template_launcher /opt/google/dataflow/python_template_launcher
WORKDIR /template
COPY orders_live.py .
ENV FLEX_TEMPLATE_PYTHON_PY_FILE=/template/orders_live.py
ARG IMAGE
ENV ORDERS_LIVE_IMAGE=${IMAGE}
ENTRYPOINT ["/opt/apache/beam/boot"]
