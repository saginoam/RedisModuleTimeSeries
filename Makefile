all:
	make -C cJSON/ clean all && make -C rmutil/ clean all && make -C timeseries/ clean all

