#!/bin/sh

function run {
    outFolder=$1
    flag=$2
    args=$3
    python evaluateCrowd.py -g conf/Config -i data/NfN_Fsu.csv -o $outFolder $args
    for f in Consensus Grouping Majority Normalized WorkerNeed; do
        mv $outFolder/$f.csv $outFolder/$f\_$flag.csv;
    done
}

run "Fsu" "nf" ""
run "Fsu" "b" "-b"
run "Fsu" "bn" "-b -n"
run "Fsu" "bnt" "-b -n -t"
run "Fsu" "bntw" "-b -n -t -w"
run "Fsu" "bntwc" "-b -n -t -w -c"
run "Fsu" "bntwcs" "-b -n -t -w -c -s"
run "Fsu" "bntwcsp" "-b -n -t -w -c -s -p"
run "Fsu" "bntwcspf" "-b -n -t -w -c -s -p -f"
run "Fsu" "bntwcspfl2" "-b -n -t -w -c -s -p -f -l 2"
run "Fsu" "bntwcspfl2v" "-b -n -t -w -c -s -p -f -l 2 -v"
run "Fsu" "bntwcspfl2va" "-b -n -t -w -c -s -p -f -l 2 -v -a"

run "MinWorkerFsu" "nfm" "-m"
run "MinWorkerFsu" "bm" "-b -m"
run "MinWorkerFsu" "bnm" "-b -n -m"
run "MinWorkerFsu" "bntm" "-b -n -t -m"
run "MinWorkerFsu" "bntwm" "-b -n -t -w -m"
run "MinWorkerFsu" "bntwcm" "-b -n -t -w -c -m"
run "MinWorkerFsu" "bntwcsm" "-b -n -t -w -c -s -m"
run "MinWorkerFsu" "bntwcspm" "-b -n -t -w -c -s -p -m"
run "MinWorkerFsu" "bntwcspfm" "-b -n -t -w -c -s -p -f -m"
run "MinWorkerFsu" "bntwcspfl2m" "-b -n -t -w -c -s -p -f -l 2 -m"
run "MinWorkerFsu" "bntwcspfl2vm" "-b -n -t -w -c -s -p -f -l 2 -v -m"
run "MinWorkerFsu" "bntwcspfl2vam" "-b -n -t -w -c -s -p -f -l 2 -v -a -m"

python scoreCsvs.py -a Fsu3/Consensus_bntwcspfl2v.csv -b data/Consensus_GoldenExpert.csv -c 1-10 -r 300 -o Fsu3/Score_bntwcspfl2v.csv
python scoreCsvs.py -a Fsu3/Consensus_bntwcspfl2va.csv -b data/Consensus_GoldenExpert.csv -c 1-10 -r 300 -o Fsu3/Score_bntwcspfl2va.csv
python scoreCsvs.py -a MinWorkerFsu3/Consensus_bntwcspfl2vm.csv -b data/Consensus_GoldenExpert.csv -c 1-10 -r 300 -o MinWorkerFsu3/Score_bntwcspfl2vm.csv
