for i in $(seq 1 100)
do 
./task2-nico-hoff/build/dev/clt -p 1026 -d 0 -m 1025 -o PUT -k $i -v 1000
done
