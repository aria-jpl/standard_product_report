FROM hysds/pge-base:latest
MAINTAINER jlinick "Justin.P.Linick@jpl.nasa.gov"
LABEL description="Standard Product Report"

USER ops

# copy packages
COPY . /home/ops/verdi/ops/standard_product_report
RUN set -ex \
 && source /home/ops/verdi/bin/activate \
 && sudo chown -R ops:ops /home/ops/verdi/ops/standard_product_report

RUN sudo pip install matplotlib

WORKDIR /home/ops
CMD ["/bin/bash", "--login"]
