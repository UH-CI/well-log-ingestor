Automated Ingest and Annotation Agave scripts for Hawai'i Well data and logs from the Hawai'i Institute of Geophysics & Planetology (HIGP)
Well Data can be found here - https://www.higp.hawaii.edu/hggrc/wells/wells.php

These scripts utilize the AGAVE Command Line Interface(CLI) (https://bitbucket.org/tacc-cic/cli) to pull data from the HIGP Well data repository and generate Well Metadata objects in the Ike Wai Gateway as well as importing the related well data files.  The Agave CLI needs to be installed and the path available to the scripts.

## STEP 1 - Store Wells
The store-wells.py script parses a csv file that has all the Well definitions and related metadata and creates new Well Metadata objects in Agave. This data comes from CWRM so we cannot share it but the required fields are long83dd, lat83dd, and wcr any other header field names/values are also imported but the 3 required are directly referenced in the script so MUST exist for it to work.  NOTE that you must modify your AGAVE TOKEN and the Well schemaid to work with your Agave instance as well as the input filename (wells.csv is the default).

After configuration you can run:
```python store-wells.py```

## STEP 2 - Ingest Well Files
The ingest_higpy.py script will download an island worth of well log files from the HIGP server and store them in Agave in the specified Agave storage system.  Prior to running you need to set the island name, AGAVE Token and system. You system will also need a folder called Well_Data in the defined home directory as this is where the script will attempt to push all files

After configuration you can run:
```python ingest_higp.py```

You can run this for each island desired to import the corresponding files.

## STEP 3 - Annoate the Well Files
The annotate_higp.py script assumes that Well objects have already been generated and files have been imported into Agave.  This script can then generate appropriate File and Data Descriptor Metadata objects to to enable the well data file annotations (This means a Data descriptor is generated and then the file is associated with it and the well is also associated - this annotates it as well as makes it possible to do text searches and spatial searches that the file would not show up in).  NOTE that the metadata schema/constructs are specific to the Ike Wai Gateway but can be created in any Agave instance, however full-text and spatial indexing may not be enabled in your Agave instance yet.

To use this there a few things that have to be configured:
#define the tenant specific schema uuids - development will be differnet from production
file_schema_uuid - the UUID of the File metadata schema

data_descriptor_schema_uuid - the UUID of the DataDescriptor metadata schema

published_file_schema_uuid - The UUID of the Published_File metadata schema

data_descriptor_id - this is the template data descriptor object uuid - you will need to create a data descriptor object that has the fields and values you want to be the base definition of your Well files.  These definitions are copied for each new data descriptor that is generated per file.

island -the island name that corresponds to a folder in Well_Data - all files will be annotated in this folder

system -the Agave storage system that has the Well_Data folder and imported well files

After those are defined you need to run:

auth_token_refresh

This will refresh your Agave token - this will be used by default in the script. NOTE the code should be updated to use the -z flag for all the CLI calls and a Agave_Token variables BUT it isn't yet so to use as is refresh your local Agave token.

```python annotate_higp.py```

Runs the automated annotations for all files in the island folder specified.
