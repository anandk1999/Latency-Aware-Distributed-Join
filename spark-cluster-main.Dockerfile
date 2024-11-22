####################################################################################
# DO NOT MODIFY THE BELOW ##########################################################

FROM spark-cluster-common

# DO NOT MODIFY THE ABOVE ##########################################################
####################################################################################

RUN /usr/local/hadoop/bin/hdfs namenode -format

COPY ./setup-main.sh ./setup-main.sh
RUN /bin/bash setup-main.sh

COPY ./start-main.sh ./start-main.sh

CMD ["/bin/bash", "start-main.sh"]