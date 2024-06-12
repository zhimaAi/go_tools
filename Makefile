.PHONY:upgrade
upgrade:
	cd curl&&go get -u
	cd logs&&go get -u
	cd mq&&go get -u
	cd msql&&go get -u
	cd tool&&go get -u
	go get -u&&go mod tidy