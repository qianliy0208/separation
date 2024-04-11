sudo cp -r ./out-shared/libleveldb.so* /usr/local/lib
sudo cp -r ./out-static/libleveldb.a /usr/local/lib
sudo cp -r ./db /usr/local/include
sudo cp -r ./include/leveldb /usr/local/include
sudo cp -r ./port /usr/local/include
sudo cp -r ./table /usr/local/include
sudo cp -r ./util /usr/local/include
sudo ldconfig