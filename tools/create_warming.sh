#! /bin/sh

## the following line is Linux version specific.  
##  Ubuntu 12
ls -la /proc/$(pgrep beam)/fd | grep '\.sst' | cut -d " " -f 12 | sort >./warming_list.txt

## 
./cache_warm ./warming_list.txt
