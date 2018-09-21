clean:
	rm -rf ./lib
	rm -f ./lib.zip
build:
	pip install -U -r requirements.txt -t ./lib/
	cd ./lib/ && zip -r lib.zip ./* && mv lib.zip ../ && cd .. && rm -rf lib 
	
