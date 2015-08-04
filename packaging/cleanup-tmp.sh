#Clean up tmp
for x in {a..z}; do rm -f /tmp/tmp$x*.png; done
for x in {A..Z}; do rm -f /tmp/tmp$x*.png; done
for x in {0..9}; do rm -f /tmp/tmp$x*.png; done
rm -f /tmp/tmp_*.png
