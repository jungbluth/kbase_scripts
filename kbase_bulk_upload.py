#!/usr/bin/python3

# Using Workspace Metadata
# from https://kbase.github.io/kb_sdk_docs/howtos/workspace.html


#Import KBase app manager
from biokbase.narrative.jobs.appmanager import AppManager




#List all files in Staging Area

#Set up the link to the staging area helper class
from biokbase.narrative.staging.helper import Helper as StagingHelper
staging_helper = StagingHelper()

#list root directory files and all subdirectory files (default)
file_list = staging_helper.list()
print 'All files:'
print '\n'.join(file_list)



#Import FASTA File as Assembly from Staging Area
from biokbase.narrative.jobs.appmanager import AppManager
AppManager().run_app(
    "kb_uploadmethods/import_fasta_as_assembly_from_staging",
    {
        "staging_file_subdir_path": "_Test/TARA_ANE_MAG_00002.fna",
        "type": "mag",
        "min_contig_length": 500,
        "assembly_name": "TARA_ANE_MAG_00002.fna_assembly"
    },
    tag="release",
    version="1.0.25",
    cell_id="ff2abe14-0a9c-4d12-8f81-30379f8ec311",
    run_id="3f295585-1b0d-44bc-a59e-3cad29ec88ed"




# Bulk extraction of bins based on Binned Contigs object followed by genome annotation # Janaka Edirisinghe. v.o1 September 5th, 2017

import time
import pprint
import json
import sys
from biokbase.narrative.jobs.appmanager import AppManager
version = "release"
current_ws = os.environ['KB_WORKSPACE_ID']




ws = biokbase.narrative.clients.get("workspace")
my_list = ws.list_objects({'workspaces':[current_ws],'type':'KBaseMetagenomes.BinnedContigs'})

count =0
for binnedContig in my_list:
     print ("\nNow running Bin extraction on binned contig " + binnedContig[1] + "\n")
     contigOb = ws.get_objects([{'ref': str (binnedContig[6]) + '/' + str (binnedContig[0]) + '/' + str(binnedContig[4])}])[0]['data']['bins']
     data = contigOb
     assembly_name = binnedContig[1].split('.Bins')[0]
     bin_list = []
     for eachbin in range(len (contigOb)):   
         bin_hash = {}    
         bin_hash['bin_id'] = [contigOb[eachbin]['bid']]
         bin_hash['assembly_suffix'] = assembly_name + '_' + contigOb[eachbin]['bid'].split('.fasta')[0]   
         bin_list.append(bin_hash)      
     
     print ("\n\n individual bins in for binnedContig[1]")
     pprint.pprint (bin_list)   

        
     job = AppManager().run_app(
             "MetagenomeUtils/extract_bins_as_assemblies",
             {
              "binned_contig_obj_ref": str (binnedContig[6]) + '/' + str (binnedContig[0]) + '/' + str(binnedContig[4]),
              "extracted_assemblies": bin_list
             },
             tag="release",
     )
     job_result = []
     
     while True:
        time.sleep(10)
        try:
             job_result = job.state()
             if job_result['job_state'] in ['completed', 'suspend']:
                break
        except Error:
            print "Job state command failed... try again."
     
     if job_result['job_state'] != 'completed':
        print "Bin extracting Failed  : " + ",".join(job_result['status'][0:3])
     
     else:
        print "Succeeded - extract binning of :"+ binnedContig[1] +" file  "+ ",".join(job_result['status'][0:3])
        #pprint.pprint (job_result)
     print ("\n Moving to annotation step on each binned contig\n\n")
    
     for eachGenome in  range(len (bin_list)): 
         print ("Now annotating " + bin_list[eachGenome]['assembly_suffix'] + "\n")
         assemblyprefix = bin_list[eachGenome]['assembly_suffix'].split('_')[-1]
         inputContigname = assemblyprefix + '.fasta' + bin_list[eachGenome]['assembly_suffix']
         
         job = AppManager().run_app(
                "RAST_SDK/annotate_contigset",
                {
                    "input_contigset": inputContigname,
                    "scientific_name": "unknown",
                    "domain": "B",
                    "genetic_code": "11",
                    "output_genome": bin_list[eachGenome]['assembly_suffix'],
                    "call_features_rRNA_SEED": 1,
                    "call_features_tRNA_trnascan": 1,
                    "call_selenoproteins": 1,
                    "call_pyrrolysoproteins": 1,
                    "call_features_repeat_region_SEED": 1,
                    "call_features_insertion_sequences": 0,
                    "call_features_strep_suis_repeat": 1,
                    "call_features_strep_pneumo_repeat": 1,
                    "call_features_crispr": 1,
                    "call_features_CDS_glimmer3": 1,
                    "call_features_CDS_prodigal": 1,
                    "annotate_proteins_kmer_v2": 1,
                    "kmer_v1_parameters": 1,
                    "annotate_proteins_similarity": 1,
                    "resolve_overlapping_features": 1,
                    "find_close_neighbors": 1,
                    "call_features_prophage_phispy": 0
                },
                tag=version
         )  
         job_result = []

         while True:
            time.sleep(10)
            try:
                 job_result = job.state()
                 if job_result['job_state'] in ['completed', 'suspend']:
                    break
            except Error:
                print "Job state command failed... try again."

         if job_result['job_state'] != 'completed':
            print "Annotation Failed  : " + ",".join(job_result['status'][0:3])

         else:
            print "\nSucceeded - Annotation of :"+ bin_list[eachGenome]['assembly_suffix'] +" file  "+ ",".join(job_result['status'][0:3])




    ####User-defined function
    # WAIT for Job to finish
import time
def wait_for_results(job,obj_name,app_name):
    if job is None:
        print "Failed - " + app_name + " for " + obj_name + " failed to return job handle"
    else:
        while job.state()['job_state'] not in ['completed', 'suspend']:
            time.sleep(5)
        job_result = job.state()
        if job_result['job_state'] != 'completed':
            print "Failed - " + app_name + " for " + obj_name + " job did not complete: " + ",".join(job_result['status'][0:3])
        else:
            print "Succeeded - " + app_name + " for " + obj_name + ",".join(job_result['status'][0:3])
    return