#Clean up production data which was not updated longer then 1 day
find /idata_prod/ -maxdepth 1 -type d -mtime +7 -exec rm -rf {} \;
#Clean up analysis data which was not updated longer then 1 day
find /dataana/ -maxdepth 1 -type d -mtime +1 -exec rm -rf {} \;
#Clean up analysis crab2 data which was not updated longer then 1 day
find /datacrab2/ -maxdepth 1 -type d -mtime +1 -exec rm -rf {} \;


