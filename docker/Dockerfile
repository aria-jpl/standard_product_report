FROM hysds/pge-isce_giant
MAINTAINER jlinick "Justin.P.Linick@jpl.nasa.gov"
LABEL description="Standard Product Report"

USER ops

RUN sudo pip install openpyxl

# copy packages
COPY . /home/ops/verdi/ops/standard_product_report
RUN set -ex \
 && source /home/ops/verdi/bin/activate \
 && sudo chown -R ops:ops /home/ops/verdi/ops/standard_product_report

WORKDIR /home/ops
CMD ["/bin/bash", "--login"]
