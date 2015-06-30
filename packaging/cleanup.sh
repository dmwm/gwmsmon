#Clean up production data which was not updated longer then 1 day
find /dataprod/ -maxdepth 1 -type d -mtime +1 -exec rm -rf {} \;
#Clean up analysis data which was not updated longer then 1 day
find /dataana/ -maxdepth 1 -type d -mtime +1 -exec rm -rf {} \;

#Clean up tmp 
for x in {a..z}; do rm -f /tmp/tmp$x*.png; done
for x in {A..Z}; do rm -f /tmp/tmp$x*.png; done
for x in {0..9}; do rm -f /tmp/tmp$x*.png; done
