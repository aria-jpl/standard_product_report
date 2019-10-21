FROM hysds/pge-base:latest

MAINTAINER jlinick "Justin.P.Linick@jpl.nasa.gov"
LABEL description="Standard Product Report"

USER root

# install openpyxl
RUN /home/ops/verdi/bin/pip install openpyxl

USER ops

# copy packages
COPY . /home/ops/verdi/ops/standard_product_report

WORKDIR /home/ops
CMD ["/bin/bash", "--login"]
