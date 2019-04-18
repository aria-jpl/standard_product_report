FROM hysds/pge-base:latest
MAINTAINER jlinick "Justin.P.Linick@jpl.nasa.gov"
LABEL description="Standard Product Report"

USER ops

# copy packages
COPY . /home/ops/verdi/ops/standard_product_report

# install requirements
WORKDIR /home/ops/verdi/ops/standard_product_report
RUN ["/bin/bash", "pip", "install", "-r", "requirements.txt"]

WORKDIR /home/ops
CMD ["/bin/bash", "--login"]
