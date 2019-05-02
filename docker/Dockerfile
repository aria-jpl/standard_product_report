FROM hysds/pge-base:python2
MAINTAINER jlinick "Justin.P.Linick@jpl.nasa.gov"
LABEL description="Standard Product Report"

USER ops

# install openpyxl
RUN sudo pip install openpyxl
# copy packages
COPY . /home/ops/verdi/ops/standard_product_report

WORKDIR /home/ops
CMD ["/bin/bash", "--login"]
