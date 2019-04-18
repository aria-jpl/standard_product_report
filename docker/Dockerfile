FROM hysds/pge-base:latest
MAINTAINER jlinick "Justin.P.Linick@jpl.nasa.gov"
LABEL description="Standard Product Report"

USER ops

# copy packages
COPY . /home/ops/verdi/ops/standard_product_report

# install requirements
RUN ["/bin/bash", "pip", "install", "openpyxl==2.6.2"]

WORKDIR /home/ops
CMD ["/bin/bash", "--login"]
