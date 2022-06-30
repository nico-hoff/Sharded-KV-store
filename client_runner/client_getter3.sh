for i in $(seq 1 21)
do 
./task2-nico-hoff/build/dev/clt -p 1026 -d 0 -m 1025 -o GET -k $i -v 1000
done
