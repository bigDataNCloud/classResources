#!/bin/bash

STARTDATE=$1
ENDDATE=$2

if [ "x${STARTDATE}x" == "xx" ]
then
    STARTMONTH=1
    STARTYEAR=2019
else
    FIRST=`echo ${STARTDATE} | cut -d"-" -f1`
    SECOND=`echo ${STARTDATE} | cut -d"-" -f2`
    if (( ${FIRST} < 13 ))
    then
	    STARTMONTH=${FIRST}
	    STARTYEAR=${SECOND}
    else
	    STARTMONTH=1
	    STARTYEAR=${SECOND}
    fi
fi

if [ "x${ENDDATE}x" == "xx" ]
then
    ENDMONTH=4
    ENDYEAR=2022
else
    FIRST=`echo ${ENDDATE} | cut -d"-" -f1`
    SECOND=`echo ${ENDDATE} | cut -d"-" -f2`
    if (( ${FIRST} < 13 ))
    then
	    ENDMONTH=${FIRST}
	    ENDYEAR=${SECOND}
    else
	    ENDMONTH=12
	    ENDYEAR=${SECOND}
    fi
fi

if [ "${CLOUD_SHELL}" != "true" ]
then
    echo "WARNING: Expecting this script to be run from Google Cloud Shell. It will only work if you have Google CLI (Google command line interface) installed locally."
fi

echo "Using the ${GOOGLE_CLOUD_PROJECT} project."

if [ "x${GOOGLE_CLOUD_PROJECT}x" == "xx" ]
then
    echo "WARNING: This shell has not been configured to use a default project. Access to Google Cloud services may be limited."
fi

BUCKET=${GOOGLE_CLOUD_PROJECT}_data
if [ `gcloud alpha storage ls | grep -c "gs://${BUCKET}/"` == 0 ]
then
    echo "Creating a bucket named ${BUCKET}."
    gsutil mb gs://${BUCKET}
else
    echo "Using bucket ${BUCKET}."
fi

prepareMonthData() {
    # Given a year and month, will download the file for the month and clean it.
    MONTH=$1
    YEAR=$2
    echo "Preparing data for ${MONTH}/${YEAR}"

    echo -n "Downloading..."
    URL="https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_${YEAR}_${MONTH}.zip"
    DOWNLOADED_FILE=${YEAR}-${MONTH}.zip
    curl -k --ciphers 'HIGH:!DH:!aNULL' -o ${DOWNLOADED_FILE} ${URL}
    if [ ! -f ${DOWNLOADED_FILE} ]
    then
        echo "ERROR: Cannot download data."
	return 1
    fi

    echo "Unzipping..."
    unzip ${DOWNLOADED_FILE}
    rm -f readme.html
    rm -f ${DOWNLOADED_FILE}

    echo -n "Cleaning..."
    cat *_${YEAR}_${MONTH}.csv | sed -e 's/,$//g' -e 's/"//g' > ${YEAR}-${MONTH}.csv
    rm -f *_${YEAR}_${MONTH}.csv
    
    if [ ! -f ${YEAR}-${MONTH}.csv ]
    then
	echo "ERROR: No cleaned data file exists."
        return 2
    fi

    echo "Storing..."
    gsutil cp ${YEAR}-${MONTH}.csv gs://${BUCKET}/data/flightsETL/
    rm -f ${YEAR}-${MONTH}.csv
    return 0
}

if (( ${STARTYEAR} == ${ENDYEAR} ))
then
    year=${STARTYEAR}
    for (( month=${STARTMONTH}; month <= ${ENDMONTH}; month++ ))
    do
	    prepareMonthData ${month} ${year}
    done
else
    year=${STARTYEAR}
    for (( month=${STARTMONTH}; month <= 12; month++ ))
    do
	    prepareMonthData ${month} ${year}
    done
    for (( year=${STARTYEAR}+1; year <= ${ENDYEAR}-1; year++ ))
    do
	    for (( month=1; month <= 12; month++ ))
	    do
	        prepareMonthData ${month} ${year}
	    done
    done
    year=${ENDYEAR}
    for (( month=1; month <= ${ENDMONTH}; month++ ))
    do
	    prepareMonthData ${month} ${year}
    done
fi
echo "Uploaded the following to gs://${BUCKET}/data/flightsETL"
gsutil ls -l gs://${BUCKET}/data/flightsETL
