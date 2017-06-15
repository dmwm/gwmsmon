#!/bin/sh

# Script run in acrontab cmst1
#*/15 * * * * lxplus ssh vocms0309 "/afs/cern.ch/user/c/cmst1/CondorMonitoring/JobCounter.sh &> /dev/null"
# outputfile CondorMonitoring.json CondorJobs_Workflows.json Running*.txt Pending*.txt per Type of job 
# outputdir /afs/cern.ch/user/c/cmst1/www/CondorMonitoring/

location="/data/prodview/src/CondorMonitoring/"
outputdir="/var/www/CondorMonitoring/"

cd $location

#Email if things are running slowly
if [ -f scriptRunning.run ];
then
    echo "Last JobCounter.sh is currently running. Will send an email to the admin."
    SUBJECT="[Monitoring] CondorMonitoring running slowly"
    EMAIL="justas.balcas@cern.ch"
    touch ./emailmessage.txt
    echo "Hi, Condor monitoring script is running slowly at:" > ./emailmessage.txt
    echo $location >> ./emailmessage.txt
    /bin/mail -s "$SUBJECT" "$EMAIL" < ./emailmessage.txt
    rm ./emailmessage.txt
    exit
else
    echo "JobCounter.sh started succesfully"
    touch scriptRunning.run
fi

#Run the script
python JobCounter.py &> JobCounter.log
exitstatus="$?"
echo "JobCounter.py exit status: $exitstatus"
mv *.json $outputdir
mv *.txt $outputdir
mv JobCounter.log $outputdir/LasRunLogFile.log
rm scriptRunning.run
