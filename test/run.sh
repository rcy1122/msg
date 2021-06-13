#!/bin/bash

echo "test "

./ghz --insecure \
--proto ./../common/broker/broker.proto \
--call broker.Broker.Publish \
-d '{"topic":"ttn","info":"i"}' \
-c 50 \
-n 10000 \
127.0.0.1:17000



#./ghz --insecure \
#--proto ./../common/broker/broker.proto \
#--call broker.Broker.Publish \
#-d '{"topic":"t1","info":"i"}' \
#-c 10 \
#-n 100 \
#127.0.0.1:17000
