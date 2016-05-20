#Clean up production data which was not updated longer then 7 days
find /idata_prod/ -maxdepth 1 -type d -mtime +1 -exec rm -rf {} \;
#Clean up analysis data which was not updated longer then 7 days
find /idata_crab3/ -maxdepth 2 -type d -mtime +7 -exec rm -rf {} \;
#Clean up analysis crab2 data which was not updated longer then 7 days
find /data_crab2/ -maxdepth 1 -type d -mtime +1 -exec rm -rf {} \;


# Remove summary files which were not updated longer than 1d.
find /idata_prod/ -maxdepth 2 -mmin +30 -type f -name summary.json -exec rm -rf {} \;
find /idata_crab3/ -maxdepth 2 -mmin +30 -type f -name summary.json -exec rm -rf {} \;

