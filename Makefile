define REDIS1_CONF
daemonize yes
port 6379
pidfile /tmp/redis1.pid
endef

define REDIS2_CONF
daemonize yes
port 6380
pidfile /tmp/redis2.pid
endef

export REDIS1_CONF
export REDIS2_CONF
export ZOOCFG := test/resources/zoo.cfg
test:
	echo "$$REDIS1_CONF" | redis-server -
	echo "$$REDIS2_CONF" | redis-server -
	

	node_modules/zookeeper/build/zookeeper-3.4.3/bin/zkServer.sh start ${ZOOCFG}
	sleep 10 

	./node_modules/.bin/_mocha --globals myThis,myHolder,myCallee,State_myThis --reporter spec -t 5000 -s 3000 ${TESTFILE}

	node_modules/zookeeper/build/zookeeper-3.4.3/bin/zkServer.sh stop ${ZOOCFG}

	kill `cat /tmp/redis1.pid`
	kill `cat /tmp/redis1.pid`

prepare_development:
	cd /tmp; git clone https://github.com/antirez/redis.git; cd redis; git checkout 5471b8babddbb99a50010593483f24187e51981a; make install;
	cd /tmp; wget http://nodejs.org/dist/v0.6.16/node-v0.6.16.tar.gz; tar -xvzf node-v0.6.16.tar.gz; cd node-v0.6.16; ./configure && make && make install;
	npm install;

run_development:
	redis-server &
	./bin/http_launcher.js

.PHONY: test
