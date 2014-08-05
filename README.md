CrowdConsensus
==============

Tool for reaching consensus on crowdsourced transcriptions and to minimize the number of workers needed by crowdsourcing tasks.

Summary:
Crowdsourcing can be a cost-effective method for tackling the problem of digitizing historical biocollections data, and a number of crowdsourcing platforms have been developed to facilitate interaction with the public and to design simple "Human Intelligence Tasks". However, the problem of reaching consensus on the response of the crowd is still challenging for tasks for which a simple majority vote is inadequate. The paper in [1] (a) describes the challenges faced when trying to reach consensus on data transcribed by different workers, (b) offers consensus algorithms for textual data and a consensus-based controller to assign a dynamic number of workers per task, and (c) proposes further enhancements of future crowdsourcing tasks in order to minimize the need for complex consensus algorithms. Experiments using the proposed algorithms show up to a 45-fold increase in ability to reach consensus when compared to majority voting using exact string matching. In addition, the worker controller is able to decrease the crowdsourcing cost by 55% when compared to a strategy that uses a fixed number of workers. This is code backing up the results in [1].

Getting started:

To reproduce the results in [1], simply execute the existing bash script, assuming you have python installed:
cd src
./runAllCases.sh

The scripts assume all crowdsourcing data to be formatted as CSV (Comma-Separated Values), and the configuration can be changed editing the files in the conf folder. All scripts have help messages to explain available command-line flags when using the option "-h".

[1] Andréa Matsunaga, Austin Mast, and José A.B. Fortes, "Reaching Consensus in Crowdsourced Transcription of Biocollections Information," in Proceedings of the 2014 IEEE 10th International Conference on e-Science, Guarujá, Brazil, 2014.
