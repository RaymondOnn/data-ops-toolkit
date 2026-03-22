1. Sometimes, there is no change to the ExtractStep, so wondering if we could extract once with the baseline job, then reuse the extracted data in the test job.
2. Regression test can involve one or more datasets
3. Ability to figure out which datasets are affected by the changes
4. Regression test is conducted via GitHub Actions
5. We will need to get the JobContext first before we can proceed to cloning or comparing results
6. For cloning, we only need to clone the skeleton/schema, not the actual data
7. For comparing results,
    - For data ingestion, we compare and ensure the data is the same
    - For file ingestion, we compare and ensure the files are the same
8. Need to be mindful not to clear the source files so that the test job can still run
9. Github Actions will also trigger the clean up of the created skeleton