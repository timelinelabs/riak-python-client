RIAK_CONF = ${RIAK_DIR}/etc/riak.conf
# ADVANCED_CONF = ${RIAK_DIR}/etc/advanced.config
# RIAK = ${RIAK_DIR}/bin/riak
RIAK_ADMIN = ${RIAK_DIR}/bin/riak-admin
# CERTS_DIR = $(shell pwd)/../src/test/resources

preconfigure:
	@../setup.py preconfigure --riak-conf=${RIAK_CONF}

configure:
	@../setup.py configure --riak-admin=${RIAK_ADMIN}

compile:
	-@yes y | pip uninstall riak-pb protobuf pyOpenSSL
	@../setup.py develop

lint:
	@pip install --upgrade pep8 pyflakes
	@cd ..; pep8 riak *.py
	@cd ..; pyflakes riak *.py

test: test_normal test_security

test_normal:
	@echo "Testing Riak Python Client (without security)"
	@../setup.py disable_security --riak-admin=${RIAK_ADMIN}
	@RUN_YZ=1 SKIP_DATATYPES=0 SKIP_INDEXES=0 ../setup.py test

test_security:
	@echo "Testing Riak Python Client (with security)"
	@../setup.py enable_security --riak-admin=${RIAK_ADMIN}
	(cd ..; RUN_YZ=1 SKIP_INDEXES=0 RUN_SECURITY=1 SKIP_POOL=1 SKIP_RESOLVE=1 RIAK_TEST_HTTP_PORT=18098 ./setup.py test)
